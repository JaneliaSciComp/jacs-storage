package org.janelia.jacsstorage.service.s3.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.enterprise.context.ApplicationScoped;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;

@ApplicationScoped
public class S3AdapterProviderImpl implements S3AdapterProvider {

    private static final ConcurrentMap<String, S3Adapter> S3_ADAPTERS = new ConcurrentHashMap<>();

    @Override
    public S3Adapter getS3Adapter(String bucket, String endpoint, JADEOptions s3Options) {
        return S3_ADAPTERS.computeIfAbsent(makeAdapterKey(bucket, endpoint, s3Options), (k) -> {
            // extract bucket and endpoint from the key
            String[] parts = k.split("#");
            return new S3Adapter(parts[0], parts[1], s3Options);
        });
    }

    private String makeAdapterKey(String bucket, String endpoint, JADEOptions s3Options) {
        StringBuilder b = new StringBuilder(bucket).append('#');
        if (StringUtils.isNotBlank(endpoint)) {
            b.append(endpoint);
        }
        b.append('#').append(StringUtils.defaultIfBlank(s3Options.getAsString("AWSRegion"), ""));
        return b.toString();
    }

}
