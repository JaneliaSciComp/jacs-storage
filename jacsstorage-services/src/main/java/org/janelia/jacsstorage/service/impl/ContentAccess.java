package org.janelia.jacsstorage.service.impl;

import org.janelia.jacsstorage.service.ContentStorageService;

class ContentAccess<S extends ContentStorageService> {

    final S storageService;
    final String contentKey;

    ContentAccess(S storageService, String contentKey) {
        this.storageService = storageService;
        this.contentKey = contentKey;
    }
}
