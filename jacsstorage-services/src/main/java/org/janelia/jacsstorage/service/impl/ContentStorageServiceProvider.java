package org.janelia.jacsstorage.service.impl;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContentStorageServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageServiceProvider.class);

    ContentAccess<? extends ContentStorageService> getStorageService(JADEStorageURI storageURI) {
        LOG.info("Get content access for {}", storageURI);
        if (storageURI == null) {
            return null;
        }
        if (storageURI.getStorageType() == JacsStorageType.S3) {
            return createS3StorageServiceInstance(storageURI);
        } else {
            return createFileStorageServiceInstance(storageURI);
        }
    }

    private ContentAccess<FileSystemStorageService> createFileStorageServiceInstance(JADEStorageURI storageURI) {
        return new ContentAccess<>(new FileSystemStorageService(), storageURI.getContentKey());
    }

    private ContentAccess<S3StorageService> createS3StorageServiceInstance(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            // use only the bucket to instantiate the storage service
            return new ContentAccess<>(new S3StorageService(storageURI.getContentBucket()), storageURI.getContentKey());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            String s3Bucket = storageURI.getContentBucket();
            String contentKey = storageURI.getContentKey();
            // use the full endpoint to instantiate the storage service
            return new ContentAccess<>(
                    new S3StorageService(storageURI.getStorageEndpoint(), s3Bucket, storageURI.getUserAccessKey(), storageURI.getUserSecretKey()),
                    contentKey
            );
        } else {
            throw new IllegalArgumentException("Cannot create S3 storage service instance for " + storageURI);
        }
    }

}
