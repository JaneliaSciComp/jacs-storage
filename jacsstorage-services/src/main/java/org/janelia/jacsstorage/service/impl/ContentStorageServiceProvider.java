package org.janelia.jacsstorage.service.impl;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentStorageService;

public class ContentStorageServiceProvider {

    ContentStorageService getStorageService(JADEStorageURI storageURI) {
        switch (storageURI.getStorageType()) {
            case S3: return createS3StorageServiceInstance(storageURI);
            default: return createFileStorageServiceInstance(storageURI);
        }
    }

    private S3StorageService createFileStorageServiceInstance(JADEStorageURI storageURI) {

        return null; // !!!
    }

    private FileSystemStorageService createS3StorageServiceInstance(JADEStorageURI storageURI) {
        return new FileSystemStorageService();
    }

}
