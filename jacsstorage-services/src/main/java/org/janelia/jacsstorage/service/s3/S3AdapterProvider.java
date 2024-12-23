package org.janelia.jacsstorage.service.s3;

import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;

public interface S3AdapterProvider {
    S3Adapter getS3Adapter(String bucket, String endpoint, JADEOptions s3Options);
}
