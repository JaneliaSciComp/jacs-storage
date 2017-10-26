package org.janelia.jacsstorage.client;

import com.google.common.hash.Hashing;
import com.google.common.hash.HashingInputStream;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataTransferService;
import org.janelia.jacsstorage.service.StorageMessageHeader;
import org.janelia.jacsstorage.service.StorageMessageResponse;
import org.janelia.jacsstorage.service.TransferState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.Optional;

public class StorageClientHttpImpl implements StorageClient {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientHttpImpl.class);

    private final DataTransferService clientStorageProxy;
    private final StorageClientImplHelper clientImplHelper;

    StorageClientHttpImpl(DataTransferService clientStorageProxy) {
        this.clientStorageProxy = clientStorageProxy;
        clientImplHelper = new StorageClientImplHelper();
    }

    @Override
    public StorageMessageResponse ping(String connectionInfo) throws IOException {
        return clientImplHelper.ping(connectionInfo);
    }

    @Override
    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException {
        Path sourcePath = Paths.get(localPath);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No path found for " + localPath);
        }
        // figure out the best localservice data format
        JacsStorageFormat localDataFormat;
        if (Files.isDirectory(sourcePath)) {
            if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
                throw new IllegalArgumentException("Cannot persist directory " + localPath + " as a single file");
            }
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        } else {
            if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
                localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
            } else {
                localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
            }
        }
        return clientImplHelper.allocateStorage(storageInfo.getConnectionURL(), storageInfo, authToken)
                .flatMap((DataStorageInfo allocatedStorage) -> {
                    LOG.debug("Allocated {}", allocatedStorage);
                    String agentStorageServiceURL = allocatedStorage.getConnectionURL();
                    try {
                        // initiate the localservice data read operation
                        TransferState<StorageMessageHeader> localDataTransfer = new TransferState<StorageMessageHeader>().setMessageType(new StorageMessageHeader(
                                allocatedStorage.getId(),
                                authToken,
                                DataTransferService.Operation.RETRIEVE_DATA,
                                localDataFormat,
                                localPath,
                                ""));
                        clientStorageProxy.beginDataTransfer(localDataTransfer);
                        return localDataTransfer.getDataReadChannel().flatMap(dataReadChannel -> clientImplHelper.streamDataToStore(agentStorageServiceURL, allocatedStorage, Channels.newInputStream(dataReadChannel), authToken));
                    } catch (IOException e) {
                        LOG.error("Error persisting the bundle {}", allocatedStorage, e);
                        return Optional.empty();
                    }
                })
                .map((DataStorageInfo dsi) -> new StorageMessageResponse(StorageMessageResponse.OK, ""))
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error allocating storage for " + storageInfo));
    }

    @Override
    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException {
        JacsStorageFormat localDataFormat;
        if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
            Files.createDirectories(Paths.get(localPath).getParent());
            localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        } else {
            // expand everything locally
            Files.createDirectories(Paths.get(localPath));
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        }
        return clientImplHelper.retrieveStorageInfo(storageInfo.getConnectionURL(), storageInfo, authToken)
                .flatMap((DataStorageInfo persistedStorageInfo) -> {
                    LOG.info("Data storage info: {}", persistedStorageInfo);
                    String agentStorageServiceURL = persistedStorageInfo.getConnectionURL();
                    return clientImplHelper.streamDataFromStore(agentStorageServiceURL, persistedStorageInfo, authToken);
                })
                .flatMap((InputStream dataStream) -> {
                    try {
                        // initiate the localservice data write operation
                        TransferState<StorageMessageHeader> localDataTransfer = new TransferState<StorageMessageHeader>().setMessageType(new StorageMessageHeader(
                                0L, // not important
                                "", // not important
                                DataTransferService.Operation.PERSIST_DATA,
                                localDataFormat,
                                localPath,
                                ""));
                        clientStorageProxy.beginDataTransfer(localDataTransfer);
                        return localDataTransfer.getDataWriteChannel()
                                .map(dataWriteChannel -> {
                                    try {
                                        HashingInputStream hashingDataStream = new HashingInputStream(Hashing.sha256(), dataStream);
                                        ByteStreams.copy(hashingDataStream, Channels.newOutputStream(dataWriteChannel));
                                        return new StorageMessageResponse(StorageMessageResponse.OK, "");
                                    } catch (IOException e) {
                                        return new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage());
                                    }
                                });
                    } catch (IOException e) {
                        return Optional.of(new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage()));
                    }
                })
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error streaming data from " + storageInfo));
    }

}
