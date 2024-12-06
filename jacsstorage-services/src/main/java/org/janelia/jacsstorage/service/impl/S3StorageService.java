package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageService implements ContentStorageService {

    private final static Logger LOG = LoggerFactory.getLogger(S3StorageService.class);

    private final S3Adapter s3Adapter;

    S3StorageService(String bucket, String endpoint, String region, String accessKey, String secretKey) {
        this.s3Adapter = new S3Adapter(bucket, endpoint, region, accessKey, secretKey);
    }

    @Override
    public boolean canAccess(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);
        LOG.debug("Check access to {}", s3Location);
        // we cannot simply do a head request because that only works for existing objects
        // and contentLocation may be a prefix
        ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                .bucket(s3Adapter.getBucket())
                .prefix(s3Location)
                .maxKeys(1)
                .build();
        try {
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getS3Client().listObjectsV2Paginator(initialRequest);
            for (ListObjectsV2Response r : listObjectsResponses) {
                if (!r.contents().isEmpty()) {
                    return true;
                }
            }
        } catch (S3Exception e) {
            return false;
        }
        return false;
    }

    public List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams contentAccessParams) {
        String s3Location = adjustLocation(contentLocation);
        LOG.debug("List content {} with {}", s3Location, contentAccessParams);

        if (contentAccessParams.isDirectoriesOnly()) {
            return listPrefixNodes(StringUtils.appendIfMissing(s3Location, "/"), contentAccessParams);
        } else {
            return listObjectNodes(s3Location, contentAccessParams);
        }
    }

    private List<ContentNode> listPrefixNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List prefix nodes at {}", s3Location);

        Queue<String> prefixQueue = new LinkedList<>();
        prefixQueue.add(s3Location);

        List<ContentNode> results = new ArrayList<>();
        while (!prefixQueue.isEmpty()) {
            String currentPrefix = prefixQueue.poll();
            String relativePrefix = StringUtils.removeEnd(
                    StringUtils.removeStart(currentPrefix.substring(s3Location.length()), '/'),
                    "/");
            int currentDepth = StringUtils.isEmpty(relativePrefix) ? 0 : StringUtils.countMatches(relativePrefix, '/') + 1;
            if (contentAccessParams.getMaxDepth() >= 0 && currentDepth > contentAccessParams.getMaxDepth()) {
                return results;
            }

            ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                    .bucket(s3Adapter.getBucket())
                    .prefix(currentPrefix)
                    .delimiter("/")
                    .build();

            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getS3Client().listObjectsV2Paginator(initialRequest);
            for (ListObjectsV2Response r : listObjectsResponses) {
                for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                    results.add(createPrefixNode(commonPrefix));
                    prefixQueue.add(commonPrefix.prefix());
                }
            }
        }

        return results;
    }

    private List<ContentNode> listObjectNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List object nodes at {} with {}", s3Location, contentAccessParams);

        Queue<String> prefixQueue = new LinkedList<>();
        prefixQueue.add(s3Location);

        long requestOffset = contentAccessParams.getStartEntryIndex();

        long currentOffset = 0;
        boolean exactMatchFound = false;
        List<ContentNode> results = new ArrayList<>();
        while (!prefixQueue.isEmpty()) {
            String currentPrefix = prefixQueue.poll();
            String relativePrefix = StringUtils.removeEnd(
                    StringUtils.removeStart(currentPrefix.substring(s3Location.length()), '/'),
                    "/");
            int currentDepth = StringUtils.isEmpty(relativePrefix) ? 0 : StringUtils.countMatches(relativePrefix, '/') + 1;
            if (contentAccessParams.getMaxDepth() >= 0 && currentDepth > contentAccessParams.getMaxDepth()) {
                return results;
            }

            ListObjectsV2Request initialRequest = ListObjectsV2Request.builder()
                    .bucket(s3Adapter.getBucket())
                    .prefix(currentPrefix)
                    .delimiter("/")
                    .build();
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getS3Client().listObjectsV2Paginator(initialRequest);
            for (ListObjectsV2Response r : listObjectsResponses) {

                if (currentOffset + r.contents().size()  <= requestOffset) {
                    currentOffset += r.contents().size();
                    for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                        prefixQueue.add(commonPrefix.prefix());
                    }
                    continue;
                }

                for (S3Object s3Object : r.contents()) {
                    currentOffset++;
                    if (currentOffset <= requestOffset) {
                        continue;
                    }

                    if (s3Object.key().equals(s3Location)) exactMatchFound = true;

                    if (contentAccessParams.matchEntry(s3Object.key())) {
                        results.add(createObjectNode(s3Object));
                    }
                    if (contentAccessParams.getEntriesCount() > 0 && results.size() >= contentAccessParams.getEntriesCount() || exactMatchFound) {
                        return results;
                    }
                }

                for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                    results.add(createPrefixNode(commonPrefix));
                    prefixQueue.add(commonPrefix.prefix());
                }
            }
        }

        return results;
    }

    private ContentNode createObjectNode(S3Object s3Object) {
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
                    .setCollection(false)
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    private ContentNode createPrefixNode(CommonPrefix s3Prefix) {
        try {
            String key = StringUtils.removeEnd(s3Prefix.prefix(), "/");
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
                    .setName(name + "/")
                    .setPrefix(prefix)
                    .setSize(0)
                    .setCollection(true)
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
        return normalizedNonBlank(StringUtils.removeStart(contentLocation, '/'));
    }

    private String normalizedNonBlank(String s) {
        return StringUtils.isBlank(s) ? "" : s;
    }
}
