package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public interface DataStorageService {
    TransferInfo persistDataStream(String dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException;
    TransferInfo retrieveDataStream(String dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException;
    List<DataNodeInfo> listDataEntries(String dataPath, JacsStorageFormat dataStorageFormat, int depth);
    void createDirectoryEntry(String dataPath, String entryName, JacsStorageFormat dataStorageFormat) throws IOException;
    long readDataEntryStream(String dataPath, String entryName, JacsStorageFormat dataStorageFormat, OutputStream outputStream) throws IOException;
    void deleteStorage(String dataPath) throws IOException;
    void cleanupStoragePath(String dataPath) throws IOException;
}
