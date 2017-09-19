package org.janelia.jacsstorage.client;

import org.janelia.jacsstorage.datarequest.DataStorageInfo;

import java.io.IOException;

public interface StorageClient {
    void persistData(String localPath, DataStorageInfo storageInfo) throws IOException;
    void retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException;
}
