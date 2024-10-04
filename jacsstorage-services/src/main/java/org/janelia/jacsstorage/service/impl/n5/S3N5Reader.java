package org.janelia.jacsstorage.service.impl.n5;

import com.google.gson.GsonBuilder;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.saalfeldlab.n5.N5KeyValueReader;

public class S3N5Reader extends N5KeyValueReader {

    public S3N5Reader(String endpoint, String region, String bucket, String accessKey, String secretKey, String basePrefix) {
        super(new S3KeyValueAccess(new S3Adapter(endpoint, region, bucket, accessKey, secretKey), basePrefix),
                basePrefix,
                new GsonBuilder(),
                false);
    }

    public S3N5Reader(String region, String bucket, String basePrefix) {
        super(new S3KeyValueAccess(new S3Adapter(region, bucket), basePrefix),
                basePrefix,
                new GsonBuilder(),
                false);
    }

}
