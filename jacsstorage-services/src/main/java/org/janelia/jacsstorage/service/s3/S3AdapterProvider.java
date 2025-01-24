package org.janelia.jacsstorage.service.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Singleton;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;

@Singleton
public class S3AdapterProvider {
    private static final ConcurrentMap<String, S3Adapter> S3_ADAPTERS = new ConcurrentHashMap<>();

    public S3Adapter getS3Adapter(String bucket, String endpoint, JADEOptions s3Options) {
        return S3_ADAPTERS.computeIfAbsent(makeAdapterKey(bucket, endpoint), (k) -> {
            // extract bucket and endpoint from the key
            String[] parts = k.split("#");
            String b;
            String e;
            if (parts.length == 1) {
                b = parts[0];
                e = null;
            } else if (parts.length == 2) {
                b = parts[0];
                e = parts[1];
            } else {
                throw new IllegalArgumentException("Invalid key " + k);
            }
            return new S3Adapter(b, e, s3Options);
        });
    }

    private String makeAdapterKey(String bucket, String endpoint) {
        StringBuilder b = new StringBuilder(bucket);
        if (StringUtils.isNotBlank(endpoint)) {
            b.append('#').append(endpoint);
        }
        return b.toString();
    }
}
