package org.janelia.jacsstorage.client;

import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.service.StorageMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class StorageClientImpl implements StorageClient {
    private static final Logger LOG = LoggerFactory.getLogger(StorageClientImpl.class);

    private StorageClient storageClient;
    private final StorageClientImplHelper clientImplHelper;

    public StorageClientImpl(StorageClient storageClient) {
        this.storageClient = storageClient;
        clientImplHelper = new StorageClientImplHelper();
    }

    @Override
    public StorageMessageResponse ping(String connectionInfo) throws IOException {
        return storageClient.ping(connectionInfo);
    }

    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo) throws IOException {
        String storageServiceURL = storageInfo.getConnectionURL();
        return clientImplHelper.allocateStorage(storageServiceURL, storageInfo)
                .map(allocatedStorage -> {
                    LOG.debug("Allocated {}", allocatedStorage);
                    try {
                        StorageMessageResponse storageResponse = storageClient.persistData(localPath, allocatedStorage);
                        if (storageResponse.getStatus() == StorageMessageResponse.OK) {
                            clientImplHelper.updateStorageInfo(storageServiceURL, storageResponse.getPersistedBytes(), storageResponse.getChecksum(), allocatedStorage);
                        }
                        return storageResponse;
                    } catch (IOException e) {
                        LOG.error("Error persisting the bundle {}", allocatedStorage, e);
                        return new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage(), 0, 0, new byte[0]);
                    }
                })
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error allocating storage for " + storageInfo, 0, 0, new byte[0]));
    }

    @Override
    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException {
        String storageServiceURL = storageInfo.getConnectionURL();
        DataStorageInfo persistedStorageInfo = clientImplHelper.retrieveStorageInfo(storageServiceURL, storageInfo);
        if (persistedStorageInfo.getConnectionInfo() == null) {
            LOG.error("No connection available for retrieving {}", storageInfo);
            return new StorageMessageResponse(StorageMessageResponse.ERROR, "No connection to " + storageInfo.getName(), 0, 0, new byte[0]);
        }
        LOG.info("Data storage info: {}", persistedStorageInfo);
        return storageClient.retrieveData(localPath, persistedStorageInfo);
    }

}
