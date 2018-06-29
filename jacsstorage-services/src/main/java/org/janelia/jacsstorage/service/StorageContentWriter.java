package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;

public interface StorageContentWriter {
    long persistDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, InputStream dataStream) throws IOException;
    long writeDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, InputStream dataEntryStream);
}
