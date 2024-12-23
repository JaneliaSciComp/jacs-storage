package org.janelia.jacsstorage.service.s3.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.enterprise.context.ApplicationScoped;

import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.janelia.jacsstorage.service.s3.S3AdapterProvider;

@ApplicationScoped
public class S3AdapterProviderImpl implements S3AdapterProvider {

    private static final ConcurrentMap<String, S3Adapter> S3_ADAPTERS = new ConcurrentHashMap<>();

    public S3Adapter getS3Adapter(String bucket, String endpoint, JADEOptions s3Options) {
        return S3_ADAPTERS.computeIfAbsent(bucket, (b) -> new S3Adapter(b, endpoint, s3Options));
    }

}
