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
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageService implements ContentStorageService {

    private static class S3ContentLocation {
        final String bucket;
        final String key;

        S3ContentLocation(String bucket, String key) {
            this.bucket = bucket;
            this.key = key;
        }
    }

    private final String bucket;
    private final S3Client s3Client;

    S3StorageService(String endpoint, String bucket, String accessKey, String secretKey) {
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
        this.bucket = bucket;
        s3Client = S3Client.create();
    }

    public List<ContentNode> listContentNodes(String contentLocation, ContentFilterParams filterParams) {
        S3ContentLocation s3Location = getS3Location(contentLocation);

        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Location.bucket)
                .prefix(s3Location.key)
                .build();
        ListObjectsV2Iterable listObjectsResponses = s3Client.listObjectsV2Paginator(initialRequest);

        List<ContentNode> results = new ArrayList<>();
        for (ListObjectsV2Response r : listObjectsResponses) {
            for (S3Object s3Object : r.contents()) {
                Path keyPath = Paths.get(s3Object.key());
                Path keyRelativePath = Paths.get(s3Location.key).relativize(keyPath);
                Path parentPath = keyRelativePath.getParent();
                int currentDepth = parentPath == null ? 0 : parentPath.getNameCount();
                if (filterParams.getMaxDepth() >= 0 && currentDepth > filterParams.getMaxDepth()) {
                    return results;
                }
                if (filterParams.matchEntry(s3Object.key())) {
                    results.add(createContentNode(s3Location.bucket, s3Object));
                }
            }
        }
        return results;
    }

    private S3ContentLocation getS3Location(String contentLocation) {
        String currentContentLocation = StringUtils.startsWith(contentLocation, "/")
                ? contentLocation.substring(1)
                : contentLocation;
        if (StringUtils.isNotBlank(bucket)) {
            return new S3ContentLocation(bucket, currentContentLocation);
        } else {
            int bucketSeparator = currentContentLocation.indexOf('/');
            if (bucketSeparator == -1) {
                throw new ContentException("Content location expected to have at least 2 components " +
                        "separated by '/' - one for bucket and one for key: " + contentLocation);
            }
            String contentBucket = contentLocation.substring(bucketSeparator);
            String contentKey = contentLocation.substring(bucketSeparator + 1);
            return new S3ContentLocation(contentBucket, contentKey);
        }
    }

    private ContentNode createContentNode(String bucket, S3Object s3Object) {
        try {
            String key = s3Object.key();
            Path p = Paths.get(key);
            Path parent = p.getParent();
            return new ContentNode(new S3ObjectContentReader(s3Client, bucket, key))
                    .setName(p.getFileName().toString())
                    .setPrefix( parent != null ? parent.toString() : "")
                    .setSize(s3Object.size())
                    .setLastModified(new Date(s3Object.lastModified().toEpochMilli()))
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public long writeContent(String contentLocation, InputStream dataStream) {
        S3ContentLocation s3Location = getS3Location(contentLocation);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3Location.bucket)
                .key(s3Location.key)
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
        S3ContentLocation s3Location = getS3Location(contentLocation);

        DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                .bucket(s3Location.bucket)
                .key(s3Location.key)
                .build();

        try {
            s3Client.deleteObject(deleteObjectRequest);
        } catch (Exception e) {
            throw new ContentException("Error deleting content at " + contentLocation, e);
        }
    }
}
