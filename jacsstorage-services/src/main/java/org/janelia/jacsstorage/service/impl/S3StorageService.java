package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageService implements ContentStorageService {

    private final S3Adapter s3Adapter;

    S3StorageService(String endpoint, String region, String bucket, String accessKey, String secretKey) {
        this.s3Adapter = new S3Adapter(endpoint, region, bucket, accessKey, secretKey);
    }

    S3StorageService(String region, String bucket) {
        this.s3Adapter = new S3Adapter(region, bucket);
    }

    @Override
    public boolean canAccess(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);
        // we cannot simply do a head request because that only works for existing objects
        // and contentLocation may be a prefix
        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(s3Location)
                .maxKeys(1)
                .build();
        try {
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getS3Client().listObjectsV2Paginator(initialRequest);

            List<ContentNode> results = new ArrayList<>();
            for (ListObjectsV2Response r : listObjectsResponses) {
                for (S3Object s3Object : r.contents()) {
                    return true;
                }
            }
        } catch (S3Exception e) {
            return false;
        }
        return false;
    }

    public List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams filterParams) {
        String s3Location = adjustLocation(contentLocation);

        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(s3Location)
                .build();
        ListObjectsV2Iterable listObjectsResponses = s3Adapter.getS3Client().listObjectsV2Paginator(initialRequest);

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
            int pathSeparatorIndex = key.lastIndexOf('/');
            String prefix;
            String name;
            if (pathSeparatorIndex == -1) {
                name = key;
                prefix = "";
            } else {
                name = key.substring(pathSeparatorIndex + 1);
                prefix = key.substring(0, pathSeparatorIndex);
            }
            return new ContentNode(JacsStorageType.S3, s3Adapter.getStorageURI())
                    .setName(name)
                    .setPrefix(prefix)
                    .setSize(s3Object.size())
                    .setLastModified(new Date(s3Object.lastModified().toEpochMilli()))
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public InputStream readContent(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();
        try {
            return s3Adapter.getS3Client().getObject(getObjectRequest, ResponseTransformer.toInputStream());
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public long writeContent(String contentLocation, InputStream dataStream) {
        String s3Location = adjustLocation(contentLocation);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        try {
            byte[] dataBytes = ByteStreams.toByteArray(dataStream);
            s3Adapter.getS3Client().putObject(putObjectRequest,
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
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        try {
            s3Adapter.getS3Client().deleteObject(deleteObjectRequest);
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

}
