package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;

public interface StorageContentReader {
    List<DataNodeInfo> listDataEntries(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, int depth);
    long retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, OutputStream dataStream) throws IOException;
    long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, OutputStream outputStream) throws IOException;
}
