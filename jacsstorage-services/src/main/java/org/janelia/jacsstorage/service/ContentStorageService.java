package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.List;

public interface ContentStorageService extends ContentStreamReader {

    boolean canAccess(String contentLocation);

    ContentNode getObjectNode(String contentLocation);

    List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams filterParams);

    long writeContent(String contentLocation, InputStream inputStream);

    void deleteContent(String contentLocation);

    StorageCapacity getStorageCapacity(String contentLocation);
}
