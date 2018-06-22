package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;

public interface StorageContentReader {
    long retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException;
    long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, OutputStream outputStream) throws IOException;
}
