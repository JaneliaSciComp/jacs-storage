package org.janelia.jacsstorage.client;

import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.protocol.StorageMessageResponse;

import java.io.IOException;

public interface StorageClient {
    StorageMessageResponse ping(String connectionInfo) throws IOException;
    StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo) throws IOException;
    StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo) throws IOException;
}
