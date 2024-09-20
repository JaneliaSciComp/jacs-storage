package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.janelia.jacsstorage.io.ContentFilterParams;

public interface ContentStorageService {

    List<ContentNode> listContentNodes(String contentLocation, ContentFilterParams filterParams);

    long writeContent(String contentLocation, InputStream inputStream);

    void deleteContent(String contentLocation);
}
