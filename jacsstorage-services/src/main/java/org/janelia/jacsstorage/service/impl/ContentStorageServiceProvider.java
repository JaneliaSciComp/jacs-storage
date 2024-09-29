package org.janelia.jacsstorage.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentStorageService;

class ContentStorageServiceProvider {

    ContentStorageService getStorageService(JADEStorageURI storageURI) {
        if (storageURI == null) {
            return null;
        }
        if (storageURI.getStorageType() == JacsStorageType.S3) {
            return createS3StorageServiceInstance(storageURI);
        } else {
            return createFileStorageServiceInstance(storageURI);
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
            String s3Bucket;
            String storageKey = storageURI.getStorageKey();
            if (StringUtils.isBlank(storageKey)) {
                s3Bucket = null;
            } else {
                // first key component is the bucket
                int compSeparatorIndex = storageKey.indexOf('/');
                s3Bucket = compSeparatorIndex == -1 ? storageKey : storageKey.substring(0, compSeparatorIndex);
            }
            // use the endpoint
            return new S3StorageService(storageURI.getStorageEndpoint(), s3Bucket, storageURI.getUserAccessKey(), storageURI.getUserSecretKey());
        } else {
            throw new IllegalArgumentException("Cannot create S3 storage service instance for " + storageURI);
        }
    }

}
