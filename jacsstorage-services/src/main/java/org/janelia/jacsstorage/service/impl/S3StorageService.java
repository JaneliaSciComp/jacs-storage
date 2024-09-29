package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.StorageCapacity;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageService implements ContentStorageService {

    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final S3Client s3Client;

    S3StorageService(String endpoint, String bucket, String accessKey, String secretKey) {
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.bucket = bucket;
        s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(() -> AwsBasicCredentials.builder()
                        .accessKeyId(accessKey)
                        .secretAccessKey(secretKey)
                        .build())
                .build();
    }

    S3StorageService(String bucket) {
        this.endpoint = null;
        this.accessKey = null;
        this.secretKey = null;
        this.bucket = bucket;
        s3Client = S3Client.create();
    }

    @Override
    public boolean canAccess(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                .bucket(bucket)
                .key(s3Location)
                .build();
        try {
            HeadObjectResponse response = s3Client.headObject(headObjectRequest);
            return true;
        } catch (S3Exception e) {
            return false;
        }
    }

    public List<ContentNode> listContentNodes(String contentLocation, ContentFilterParams filterParams) {
        String s3Location = adjustLocation(contentLocation);

        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(s3Location)
                .build();
        ListObjectsV2Iterable listObjectsResponses = s3Client.listObjectsV2Paginator(initialRequest);

        List<ContentNode> results = new ArrayList<>();
        for (ListObjectsV2Response r : listObjectsResponses) {
            for (S3Object s3Object : r.contents()) {
                Path keyPath = Paths.get(s3Object.key());
                Path keyRelativePath = Paths.get(s3Location).relativize(keyPath);
                Path parentPath = keyRelativePath.getParent();
                int currentDepth = parentPath == null ? 0 : parentPath.getNameCount();
                if (filterParams.getMaxDepth() >= 0 && currentDepth > filterParams.getMaxDepth()) {
                    return results;
                }
                if (filterParams.matchEntry(s3Object.key())) {
                    results.add(createContentNode(s3Object));
                }
            }
        }
        return results;
    }

    private ContentNode createContentNode(S3Object s3Object) {
        try {
            String key = s3Object.key();
            Path p = Paths.get(key);
            Path parent = p.getParent();
            return new ContentNode(getJADEStorageURI(), new S3ObjectContentReader(s3Client, bucket, key))
                    .setName(p.getFileName().toString())
                    .setPrefix(parent != null ? parent.toString() : "")
                    .setSize(s3Object.size())
                    .setLastModified(new Date(s3Object.lastModified().toEpochMilli()))
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public long writeContent(String contentLocation, InputStream dataStream) {
        String s3Location = adjustLocation(contentLocation);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucket)
                .key(s3Location)
                .build();

        try {
            byte[] dataBytes = ByteStreams.toByteArray(dataStream);
            s3Client.putObject(putObjectRequest,
                    RequestBody.fromBytes(dataBytes)
            );
            return dataBytes.length;
        } catch (IOException e) {
            throw new ContentException("Error writing content to " + contentLocation, e);
        }
    }

    @Override
    public void deleteContent(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(bucket)
                .key(s3Location)
                .build();

        try {
            s3Client.deleteObject(deleteObjectRequest);
        } catch (Exception e) {
            throw new ContentException("Error deleting content at " + contentLocation, e);
        }
    }

    @Override
    public StorageCapacity getStorageCapacity(String contentLocation) {
        return new StorageCapacity(-1L, -1L); // don't know how to calculate it
    }

    private String adjustLocation(String contentLocation) {
        String currentContentLocation = StringUtils.removeStart(contentLocation, '/');
        return StringUtils.isBlank(currentContentLocation) ? "" : currentContentLocation;
    }

    private JADEStorageURI getJADEStorageURI() {
        StringBuilder jadeStorageURIBuilder = new StringBuilder();
        URI endpointURI;
        String appendedBucket;
        if (StringUtils.isNotBlank(endpoint)) {
            endpointURI = URI.create(endpoint);
            appendedBucket = '/' + bucket;
        } else {
            endpointURI = URI.create("s3://" + bucket);
            appendedBucket = "";
        }
        jadeStorageURIBuilder
                .append(endpointURI.getScheme()).append("://");
        if (StringUtils.isNotBlank(accessKey)) {
            jadeStorageURIBuilder.append(accessKey).append(':').append(secretKey).append('@');
        }
        jadeStorageURIBuilder.append(endpointURI.getHost()).append(appendedBucket);
        return JADEStorageURI.createStoragePathURI(jadeStorageURIBuilder.toString());
    }
}
