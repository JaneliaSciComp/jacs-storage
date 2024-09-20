package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3ObjectContentReader implements ContentReader {

    private final Logger LOG = LoggerFactory.getLogger(S3ObjectContentReader.class);

    private final S3Client s3Client;
    private final String bucket;
    private final String key;

    S3ObjectContentReader(S3Client s3Client, String bucket, String key) {
        this.s3Client = s3Client;
        this.bucket = bucket;
        this.key = key;
    }

    @Override
    public InputStream getContentInputstream() {
        try {
            LOG.trace("Read content from s3://{}/{}", bucket, key);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build();
            return s3Client.getObject(getObjectRequest, ResponseTransformer.toInputStream());
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }
}
