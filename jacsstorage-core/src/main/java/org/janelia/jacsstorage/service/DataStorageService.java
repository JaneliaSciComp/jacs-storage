package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface DataStorageService {
    TransferInfo persistDataStream(String dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException;
    TransferInfo retrieveDataStream(String dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException;
    void deleteStorage(String dataPath) throws IOException;
    void cleanupStoragePath(String dataPath) throws IOException;
}
