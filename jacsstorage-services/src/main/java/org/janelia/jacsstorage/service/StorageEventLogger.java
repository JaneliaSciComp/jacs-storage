package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;

public interface StorageEventLogger {
    JacsStorageEvent logStorageEvent(String name, String description, Object data);
}
