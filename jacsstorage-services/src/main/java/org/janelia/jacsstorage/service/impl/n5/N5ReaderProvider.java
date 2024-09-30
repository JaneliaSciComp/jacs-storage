package org.janelia.jacsstorage.service.impl.n5;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.impl.S3StorageService;
import org.janelia.saalfeldlab.n5.N5FSReader;
import org.janelia.saalfeldlab.n5.N5Reader;

public class N5ReaderProvider {

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
        return new N5FSReader(storageURI.getJadeStorage());
    }

    private S3N5Reader createN5S3Reader(JADEStorageURI storageURI) {
        if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.S3) {
            // hostname is interpreted as bucket
            return new S3N5Reader(storageURI.getStorageHost(), storageURI.getStorageKey());
        } else if (storageURI.getStorageScheme() == JADEStorageURI.JADEStorageScheme.HTTP) {
            String s3Bucket;
            String storageKey = storageURI.getStorageKey();
            String basePrefix;
            if (StringUtils.isBlank(storageKey)) {
                throw new IllegalArgumentException("Cannot get S3 bucket name from " + storageURI);
            } else {
                // first key component is the bucket
                int compSeparatorIndex = storageKey.indexOf('/');
                if (compSeparatorIndex == -1) {
                    s3Bucket = storageKey;
                    basePrefix = "";
                } else {
                    s3Bucket = storageKey.substring(0, compSeparatorIndex);
                    basePrefix = storageKey.substring(compSeparatorIndex + 1);
                }
            }
            // use the endpoint
            // use the endpoint
            return new S3N5Reader(storageURI.getStorageEndpoint(), s3Bucket, storageURI.getUserAccessKey(), storageURI.getUserSecretKey(), basePrefix);
        } else {
            throw new IllegalArgumentException("Cannot create S3 N5 reader instance for " + storageURI);
        }
    }

}
