package org.janelia.jacsstorage.service.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

@Singleton
public class S3AdapterProvider {

    private static final ConcurrentMap<String, S3Adapter> S3_ADAPTERS = new ConcurrentHashMap<>();

    public S3Adapter getS3Adapter(String bucket, String endpoint, String region, String accessKey, String secretKey) {
        return S3_ADAPTERS.computeIfAbsent(bucket, (b) -> new S3Adapter(
                b,
                endpoint,
                region,
                accessKey,
                secretKey
        ));
    }

}
