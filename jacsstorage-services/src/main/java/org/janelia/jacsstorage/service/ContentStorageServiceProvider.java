package org.janelia.jacsstorage.service;

import jakarta.annotation.Nullable;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;

public interface ContentStorageServiceProvider {
    @Nullable ContentStorageService getStorageService(@Nullable JADEStorageURI storageURI);
}
