package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

public interface DataStorageService extends StorageContentReader, StorageContentWriter {
    List<DataNodeInfo> listDataEntries(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, int depth);
    long createDirectoryEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat);
    long createFileEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream contentStream);
    void deleteStorage(Path dataPath) throws IOException;
    void cleanupStoragePath(Path dataPath) throws IOException;
}
