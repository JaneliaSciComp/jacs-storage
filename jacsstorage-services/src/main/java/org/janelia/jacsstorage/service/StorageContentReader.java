package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public interface StorageContentReader {
    Map<String, Object> getDataEntryInfo(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat);
    List<DataNodeInfo> listDataEntries(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, int depth);
    long retrieveDataStream(Path dataPath, JacsStorageFormat dataStorageFormat, ContentFilterParams filterParams, OutputStream dataStream) throws IOException;
    long readDataEntryStream(Path dataPath, String entryName, JacsStorageFormat dataStorageFormat, ContentFilterParams filterParams, OutputStream outputStream) throws IOException;
}
