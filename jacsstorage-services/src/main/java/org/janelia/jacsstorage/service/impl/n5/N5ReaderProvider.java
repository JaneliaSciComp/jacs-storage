package org.janelia.jacsstorage.service.impl.n5;

import javax.inject.Inject;

import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

public class N5ReaderProvider {

    private final String defaultAWSRegion;

    @Inject
    N5ReaderProvider(@PropertyValue(name = "AWS.Region", defaultValue = "us-east-1") String defaultAWSRegion) {
        this.defaultAWSRegion = defaultAWSRegion;
    }

    public N5Reader getN5Reader(JADEStorageURI storageURI) {
        if (storageURI == null) {
            return null;
        }
        if (storageURI.getStorageType() == JacsStorageType.S3) {
            return createN5FSReader(storageURI);
        } else {
            return createN5S3Reader(storageURI);
        }
    }

    private N5Reader createN5FSReader(JADEStorageURI storageURI) {
        return new N5FSReader(storageURI.getContentKey());
    }

    private S3N5Reader createN5S3Reader(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            return new S3N5Reader(defaultAWSRegion, storageURI.getContentBucket(), storageURI.getContentKey());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            return new S3N5Reader(
                    storageURI.getStorageEndpoint(),
                    defaultAWSRegion,
                    storageURI.getContentBucket(),
                    storageURI.getUserAccessKey(),
                    storageURI.getUserSecretKey(),
                    storageURI.getContentKey());
        } else {
            throw new IllegalArgumentException("Cannot create S3 N5 reader instance for " + storageURI);
        }
    }

}
