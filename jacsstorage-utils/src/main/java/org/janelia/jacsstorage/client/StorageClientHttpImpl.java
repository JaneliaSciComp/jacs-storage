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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class StorageClientHttpImpl implements StorageClient {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientHttpImpl.class);

    private final DataTransferService clientStorageProxy;
    private final StorageClientImplHelper clientImplHelper;

    public StorageClientHttpImpl(DataTransferService clientStorageProxy) {
        this.clientStorageProxy = clientStorageProxy;
        clientImplHelper = new StorageClientImplHelper();
    }

    @Override
    public StorageMessageResponse ping(String connectionInfo) throws IOException {
        String endpoint = "/storage/status";
        Client httpClient = null;
        try {
            httpClient = clientImplHelper.createHttpClient();
            WebTarget target = httpClient.target(connectionInfo).path(endpoint);

            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .get()
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return new StorageMessageResponse(StorageMessageResponse.OK, "", 0, 0, new byte[0]);
            } else {
                return new StorageMessageResponse(StorageMessageResponse.ERROR, "Response status: " + responseStatus, 0, 0, new byte[0]);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    @Override
    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo) throws IOException {
        Path sourcePath = Paths.get(localPath);
        JacsStorageFormat localDataFormat;
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No path found for " + localPath);
        }
        // figure out the best local data format
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
        return clientImplHelper.allocateStorage(storageInfo.getConnectionURL(), storageInfo)
                .flatMap((DataStorageInfo allocatedStorage) -> {
                    LOG.debug("Allocated {}", allocatedStorage);
                    String agentStorageServiceURL = allocatedStorage.getConnectionURL();
                    try {
                        // initiate the local data read operation
                        TransferState<StorageMessageHeader> localDataTransfer = new TransferState<StorageMessageHeader>().setMessageType(new StorageMessageHeader(
                                DataTransferService.Operation.RETRIEVE_DATA,
                                localDataFormat,
                                localPath,
                                ""));
                        clientStorageProxy.beginDataTransfer(localDataTransfer);
                        return localDataTransfer.getDataReadChannel().flatMap(dataReadChannel -> clientImplHelper.streamDataToStore(agentStorageServiceURL, allocatedStorage, Channels.newInputStream(dataReadChannel)));
                    } catch (IOException e) {
                        LOG.error("Error persisting the bundle {}", allocatedStorage, e);
                        return Optional.empty();
                    }
                })
                .map(ti -> new StorageMessageResponse(StorageMessageResponse.OK, "", ti.getNumBytes(), ti.getNumBytes(), ti.getChecksum()))
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error allocating storage for " + storageInfo, 0, 0, new byte[0]));
    }

    @Override
    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException {
        JacsStorageFormat localDataFormat;
        if (storageInfo.getStorageFormat() == JacsStorageFormat.SINGLE_DATA_FILE) {
            Files.createDirectories(Paths.get(localPath).getParent());
            localDataFormat = JacsStorageFormat.SINGLE_DATA_FILE;
        } else {
            // expand everything locally
            Files.createDirectories(Paths.get(localPath));
            localDataFormat = JacsStorageFormat.DATA_DIRECTORY;
        }
        clientImplHelper.retrieveStorageInfo(storageInfo.getConnectionURL(), storageInfo)
                .flatMap((DataStorageInfo persistedStorageInfo) -> {
                    LOG.info("Data storage info: {}", persistedStorageInfo);
                    String agentStorageServiceURL = persistedStorageInfo.getConnectionURL();
                    return clientImplHelper.streamDataFromStore(agentStorageServiceURL, persistedStorageInfo);
                })
                .flatMap((InputStream dataStream) -> {
                    try {
                        // initiate the local data write operation
                        TransferState<StorageMessageHeader> localDataTransfer = new TransferState<StorageMessageHeader>().setMessageType(new StorageMessageHeader(
                                DataTransferService.Operation.PERSIST_DATA,
                                localDataFormat,
                                localPath,
                                ""));
                        clientStorageProxy.beginDataTransfer(localDataTransfer);
                        return localDataTransfer.getDataWriteChannel()
                                .map(dataWriteChannel -> {
                                    try {
                                        HashingInputStream hashingDataStream = new HashingInputStream(Hashing.sha256(), dataStream);
                                        long nbytes = ByteStreams.copy(hashingDataStream, Channels.newOutputStream(dataWriteChannel));
                                        return new StorageMessageResponse(StorageMessageResponse.OK, "", nbytes, nbytes, hashingDataStream.hash().asBytes());
                                    } catch (IOException e) {
                                        return new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage(), 0, 0, new byte[0]);
                                    }
                                });
                    } catch (IOException e) {
                        return Optional.of(new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage(), 0, 0, new byte[0]));
                    }
                })
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error streaming data from " + storageInfo, 0, 0, new byte[0]));
        return null;
    }

}
