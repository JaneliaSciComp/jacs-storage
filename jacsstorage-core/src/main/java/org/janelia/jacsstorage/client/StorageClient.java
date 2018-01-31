package org.janelia.jacsstorage.client;

import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.datatransfer.StorageMessageResponse;

import java.io.IOException;

public interface StorageClient {
    StorageMessageResponse ping(String connectionInfo) throws IOException;
    StorageMessageResponse persistData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException;
    StorageMessageResponse retrieveData(String localPath, DataStorageInfo storageInfo, String authToken) throws IOException;
}
