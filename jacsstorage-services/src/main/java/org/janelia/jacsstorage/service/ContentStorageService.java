package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentAccessParams;

public interface ContentStorageService {

    boolean canAccess(String contentLocation);

    List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams filterParams);

    long writeContent(String contentLocation, InputStream inputStream);

    void deleteContent(String contentLocation);

    StorageCapacity getStorageCapacity(String contentLocation);
}
