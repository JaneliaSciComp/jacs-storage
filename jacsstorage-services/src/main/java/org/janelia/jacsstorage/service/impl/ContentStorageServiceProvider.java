package org.janelia.jacsstorage.service.impl;

import javax.annotation.Nullable;
import javax.inject.Inject;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ContentStorageServiceProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageServiceProvider.class);

    private final S3AdapterProvider s3AdapterProvider;
    private final String defaultAWSRegion;
    private final boolean defaultAsyncAccess;
    private final boolean tryAnonymousAccessFirst;
    private final int apiBufferSizeInMiB;
    private final int minPartSizeInMiB;

    @Inject
    ContentStorageServiceProvider(S3AdapterProvider s3AdapterProvider,
                                  @PropertyValue(name = "AWS.Region.Default", defaultValue = "us-east-1") String defaultAWSRegion,
                                  @PropertyValue(name = "AWS.AsyncAccess.Default", defaultValue = "false") boolean defaultAsyncAccess,
                                  @PropertyValue(name = "AWS.TryAnonymousAccessFirstIfNoCredentialsProvided.Default", defaultValue = "false") boolean tryAnonymousAccessFirst,
                                  @PropertyValue(name = "AWS.ApiCallBufferInMiB.Default", defaultValue = "1024") int apiBufferSizeInMiB,
                                  @PropertyValue(name = "AWS.MinPartSizeInMiB.Default", defaultValue = "384") int minPartSizeInMiB) {
        this.s3AdapterProvider = s3AdapterProvider;
        this.defaultAWSRegion = defaultAWSRegion;
        this.defaultAsyncAccess = defaultAsyncAccess;
        this.tryAnonymousAccessFirst = tryAnonymousAccessFirst;
        this.apiBufferSizeInMiB = apiBufferSizeInMiB;
        this.minPartSizeInMiB = minPartSizeInMiB;
    }

    @Nullable ContentStorageService getStorageService(@Nullable JADEStorageURI storageURI) {
        LOG.trace("Get storage service for {}", storageURI);
        if (storageURI == null) {
            return null;
        } else if (storageURI.getStorageType() == JacsStorageType.S3) {
            return createS3StorageServiceInstance(storageURI);
        } else {
            return createFileStorageServiceInstance();
        }
    }

    private ContentStorageService createFileStorageServiceInstance() {
        return new FileSystemStorageService();
    }

    private ContentStorageService createS3StorageServiceInstance(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            // use only the bucket to instantiate the storage service
            S3Adapter s3Adapter = s3AdapterProvider.getS3Adapter(
                    storageURI.getContentBucket(),
                    null,
                    storageURI.getStorageOptions()
                            .setDefaultAWSRegion(defaultAWSRegion)
                            .setDefaultPathStyleBucket(false)
                            .setDefaultAsyncAccess(defaultAsyncAccess)
                            .setDefaultTryAnonymousAccessFirst(tryAnonymousAccessFirst),
                    apiBufferSizeInMiB,
                    minPartSizeInMiB
            );
            return createS3StorageServiceInstance(s3Adapter, storageURI.getStorageOptions().getAsyncAccess());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            String s3Bucket = storageURI.getContentBucket();
            // use the full endpoint to instantiate the storage service
            S3Adapter s3Adapter = s3AdapterProvider.getS3Adapter(
                    s3Bucket,
                    storageURI.getStorageEndpoint(),
                    storageURI.getStorageOptions()
                            .setDefaultAWSRegion(defaultAWSRegion)
                            .setDefaultPathStyleBucket(true)
                            .setDefaultAsyncAccess(defaultAsyncAccess)
                            .setDefaultTryAnonymousAccessFirst(tryAnonymousAccessFirst),
                    apiBufferSizeInMiB,
                    minPartSizeInMiB
            );
            return createS3StorageServiceInstance(s3Adapter, storageURI.getStorageOptions().getAsyncAccess());
        } else {
            throw new IllegalArgumentException("Cannot create S3 storage service instance for " + storageURI);
        }
    }

    private ContentStorageService createS3StorageServiceInstance(S3Adapter s3Adapter, boolean useAsync) {
        return useAsync ? new AsyncS3StorageService(s3Adapter) : new SyncS3StorageService(s3Adapter);
    }
}
