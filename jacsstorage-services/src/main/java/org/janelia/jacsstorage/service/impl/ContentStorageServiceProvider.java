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

    private FileSystemStorageService createFileStorageServiceInstance(JADEStorageURI storageURI) {
        return new FileSystemStorageService();
    }

    private S3StorageService createS3StorageServiceInstance(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            // hostname is interpreted as bucket
            return new S3StorageService(storageURI.getStorageHost());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            // use the endpoint
            return new S3StorageService(storageURI.getStorageEndpoint(), storageURI.getUserAccessKey(), storageURI.getUserSecretKey());
        } else {
            throw new IllegalArgumentException("Cannot create S3 storage service instance for " + storageURI);
        }
    }

}
