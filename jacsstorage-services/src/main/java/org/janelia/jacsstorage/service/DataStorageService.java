package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

public interface DataStorageService extends StorageContentReader, StorageContentWriter {
    long createDirectoryEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat);
    long createFileEntry(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream contentStream);
    void deleteStorage(Path dataPath) throws IOException;
    void cleanupStoragePath(Path dataPath) throws IOException;
}
