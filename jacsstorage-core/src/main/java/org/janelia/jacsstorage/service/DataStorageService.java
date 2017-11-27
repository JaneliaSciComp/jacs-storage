package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

public interface DataStorageService {
    TransferInfo persistDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException;
    TransferInfo retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException;
    List<DataNodeInfo> listDataEntries(Path dataPath, JacsStorageFormat dataStorageFormat, int depth);
    long createDirectoryEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat);
    long createFileEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream contentStream);
    long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, OutputStream outputStream) throws IOException;
    void deleteStorage(Path dataPath) throws IOException;
    void cleanupStoragePath(Path dataPath) throws IOException;
}
