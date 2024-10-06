package org.janelia.jacsstorage.service.impl;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContentStorageServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageServiceProvider.class);

    private final String defaultAWSRegion;

    @Inject
    ContentStorageServiceProvider(@PropertyValue(name = "AWS.Region", defaultValue = "us-east-1") String defaultAWSRegion) {
        this.defaultAWSRegion = defaultAWSRegion;
    }

    @Nullable ContentStorageService getStorageService(@Nullable JADEStorageURI storageURI) {
        LOG.trace("Get storage service for {}", storageURI);
        if (storageURI == null) {
            return null;
        } else if (storageURI.getStorageType() == JacsStorageType.S3) {
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
            // use only the bucket to instantiate the storage service
            return new S3StorageService(defaultAWSRegion, storageURI.getContentBucket());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            String s3Bucket = storageURI.getContentBucket();
            // use the full endpoint to instantiate the storage service
            return new S3StorageService(storageURI.getStorageEndpoint(), defaultAWSRegion, s3Bucket, storageURI.getUserAccessKey(), storageURI.getUserSecretKey());
        } else {
            throw new IllegalArgumentException("Cannot create S3 storage service instance for " + storageURI);
        }
    }

}
