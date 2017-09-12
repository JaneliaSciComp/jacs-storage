package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;

public interface StorageClient {
    void persistData(String localPath, String remotePath, JacsStorageFormat remoteDataFormat) throws IOException;
    void retrieveData(String localPath, String remotePath, JacsStorageFormat remoteDataFormat) throws IOException;
}
