package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentFilterParams;

public interface ContentStorageService {

    boolean canAccess(String contentLocation);

    List<ContentNode> listContentNodes(String contentLocation, ContentFilterParams filterParams);

    long writeContent(String contentLocation, InputStream inputStream);

    void deleteContent(String contentLocation);

    StorageCapacity getStorageCapacity(String contentLocation);
}
