package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;

public interface StorageClient {
    void persistData(String source, String target, JacsStorageFormat remoteDataFormat) throws IOException;
    void retrieveData(String source, String target, JacsStorageFormat remoteDataFormat) throws IOException;
}
