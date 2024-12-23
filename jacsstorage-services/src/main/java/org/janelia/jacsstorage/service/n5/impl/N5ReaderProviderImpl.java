package org.janelia.jacsstorage.service.n5.impl;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.n5.N5ReaderProvider;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

@ApplicationScoped
public class N5ReaderProviderImpl implements N5ReaderProvider {

    private final S3AdapterProvider s3AdapterProvider;
    private final String defaultAWSRegion;

    @Inject
    public N5ReaderProviderImpl(S3AdapterProvider s3AdapterProvider,
                                @PropertyValue(name = "AWS.Region", defaultValue = "us-east-1") String defaultAWSRegion) {
        this.s3AdapterProvider = s3AdapterProvider;
        this.defaultAWSRegion = defaultAWSRegion;
    }

    public N5Reader getN5Reader(JADEStorageURI storageURI) {
        if (storageURI == null) {
            return null;
        }
        if (storageURI.getStorageType() == JacsStorageType.S3) {
            return createN5S3Reader(storageURI);
        } else {
            return createN5FSReader(storageURI);
        }
    }

    private N5Reader createN5FSReader(JADEStorageURI storageURI) {
        return new N5FSReader(storageURI.getContentKey());
    }

    private S3N5Reader createN5S3Reader(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            return new S3N5Reader(
                    s3AdapterProvider.getS3Adapter(
                            storageURI.getContentBucket(),
                            null,
                            storageURI.getStorageOptions()
                                    .setDefaultAWSRegion(defaultAWSRegion)
                                    .setDefaultPathStyleBucket(false)
                                    .setDefaultAsyncAccess(true)
                    ),
                    storageURI.getContentKey());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            return new S3N5Reader(
                    s3AdapterProvider.getS3Adapter(
                            storageURI.getContentBucket(),
                            storageURI.getStorageEndpoint(),
                            storageURI.getStorageOptions()
                                    .setDefaultAWSRegion(defaultAWSRegion)
                                    .setDefaultPathStyleBucket(true)
                                    .setDefaultAsyncAccess(false)
                    ),
                    storageURI.getContentKey());
        } else {
            throw new IllegalArgumentException("Cannot create S3 N5 reader instance for " + storageURI);
        }
    }

}
