package org.janelia.jacsstorage.client;

import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datatransfer.StorageMessageResponse;
import org.janelia.jacsstorage.utils.StorageClientImplHelper;
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

    public StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException {
        String storageServiceURL = storageInfo.getConnectionURL();
        return clientImplHelper.allocateStorage(storageServiceURL, storageInfo, authToken)
                .map(allocatedStorage -> {
                    LOG.debug("Allocated {}", allocatedStorage);
                    try {
                        return storageClient.persistData(localPath, allocatedStorage, authToken);
                    } catch (IOException e) {
                        LOG.error("Error persisting the bundle {}", allocatedStorage, e);
                        return new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage());
                    }
                })
                .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "Error allocating storage for " + storageInfo));
    }

    @Override
    public StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException {
        String storageServiceURL = storageInfo.getConnectionURL();
        return clientImplHelper.retrieveStorageInfo(storageServiceURL, storageInfo, authToken)
            .map((DataStorageInfo persistedStorageInfo) -> {
                LOG.info("Data storage info: {}", persistedStorageInfo);
                try {
                    return storageClient.retrieveData(localPath, persistedStorageInfo, authToken);
                } catch (IOException e) {
                    LOG.error("Error retrieving data from {}", persistedStorageInfo, e);
                    return new StorageMessageResponse(StorageMessageResponse.ERROR, e.getMessage());
                }
            })
            .orElse(new StorageMessageResponse(StorageMessageResponse.ERROR, "No connection to " + storageInfo.getName()));
    }

}
