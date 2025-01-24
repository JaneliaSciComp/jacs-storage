package org.janelia.jacsstorage.service.impl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.NoContentFoundException;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class SyncS3StorageService extends AbstractS3StorageService {

    private final static Logger LOG = LoggerFactory.getLogger(SyncS3StorageService.class);

    SyncS3StorageService(S3Adapter s3Adapter) {
        super(s3Adapter);
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
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getSyncS3Client().listObjectsV2Paginator(initialRequest);
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

    @Override
    public ContentNode getObjectNode(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        try {
            HeadObjectRequest contentRequest = HeadObjectRequest.builder()
                    .bucket(s3Adapter.getBucket())
                    .key(s3Location)
                    .build();

            HeadObjectResponse contentResponse = s3Adapter.getSyncS3Client().headObject(contentRequest);

            return createObjectNode(contentLocation, contentResponse.contentLength(), contentResponse.lastModified());
        } catch (NoSuchUploadException | NoSuchBucketException e) {
            throw new NoContentFoundException(e);
        }
    }


    List<ContentNode> listPrefixNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List prefix nodes at {}", s3Location);

        Queue<String> prefixQueue = new LinkedList<>();
        prefixQueue.add(s3Location);

        List<ContentNode> results = new ArrayList<>();
        int level = 0;
        while (!prefixQueue.isEmpty()) {
            String currentPrefix = prefixQueue.poll();

            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(s3Adapter.getBucket())
                    .prefix(currentPrefix)
                    .delimiter("/")
                    .build();

            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getSyncS3Client().listObjectsV2Paginator(listRequest);
            int responseIndex = 0;
            for (ListObjectsV2Response r : listObjectsResponses) {
                if (responseIndex++ == 0 && level == 0 &&
                        (!r.commonPrefixes().isEmpty() || !r.contents().isEmpty()) &&
                        currentPrefix.endsWith("/") &&
                        contentAccessParams.checkDepth(getPathDepth(s3Location, currentPrefix))) {
                    results.add(createPrefixNode(s3Location));
                }
                for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                    String newPrefix = commonPrefix.prefix();
                    if (contentAccessParams.matchEntry(newPrefix)) {
                        results.add(createPrefixNode(newPrefix));
                    }
                    if (contentAccessParams.checkDepth(getPathDepth(s3Location, newPrefix))) {
                        prefixQueue.add(newPrefix);
                    }
                }
            }
            level++;
        }

        return results;
    }

    List<ContentNode> listObjectNodes(String s3Location, ContentAccessParams contentAccessParams) {
        LOG.debug("List object nodes at {} with {}", s3Location, contentAccessParams);

        Queue<String> prefixQueue = new LinkedList<>();
        prefixQueue.add(s3Location);

        long requestOffset = contentAccessParams.getStartEntryIndex();

        long currentOffset = 0;
        List<ContentNode> results = new ArrayList<>();
        int level = 0;
        while (!prefixQueue.isEmpty()) {
            String currentPrefix = prefixQueue.poll();

            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                    .bucket(s3Adapter.getBucket())
                    .prefix(currentPrefix)
                    .delimiter("/")
                    .build();
            ListObjectsV2Iterable listObjectsResponses = s3Adapter.getSyncS3Client().listObjectsV2Paginator(listRequest);
            int responseIndex = 0;
            for (ListObjectsV2Response r : listObjectsResponses) {
                if (responseIndex++ == 0 && level == 0 &&
                        (!r.commonPrefixes().isEmpty() || !r.contents().isEmpty()) &&
                        currentPrefix.endsWith("/") &&
                        contentAccessParams.checkDepth(getPathDepth(s3Location, currentPrefix))) {
                    if (currentOffset == requestOffset && contentAccessParams.matchEntry(s3Location)) {
                        results.add(createPrefixNode(s3Location));
                    }
                    currentOffset++;
                }

                for (CommonPrefix commonPrefix : r.commonPrefixes()) {
                    if (contentAccessParams.checkDepth(getPathDepth(s3Location, commonPrefix.prefix()))) {
                        prefixQueue.add(commonPrefix.prefix());
                    }
                    if (currentOffset >= requestOffset && contentAccessParams.matchEntry(commonPrefix.prefix())) {
                        results.add(createPrefixNode(commonPrefix.prefix()));
                    }
                    currentOffset++;
                }

                if (currentOffset + r.contents().size() < requestOffset) {
                    currentOffset += r.contents().size();
                    continue;
                }

                List<S3Object> s3Objects = new ArrayList<>(r.contents());
                s3Objects.sort((s1, s2) -> ComparatorUtils.naturalCompare(s1.key(), s2.key(), true));
                for (S3Object s3Object : s3Objects) {
                    if (currentOffset++ < requestOffset) {
                        continue;
                    }

                    if (s3Object.key().equals(s3Location)) {
                        // if an exact match is found create the node and return
                        results.add(createObjectNode(s3Object));
                        return results;
                    }

                    if (contentAccessParams.matchEntry(s3Object.key())) {
                        results.add(createObjectNode(s3Object));
                    }

                    if (contentAccessParams.getEntriesCount() > 0 && results.size() >= contentAccessParams.getEntriesCount()) {
                        return results;
                    }
                }

            }
            level++;
        }

        return results;
    }

    @Override
    public InputStream getContentInputStream(String contentLocation) {
        String s3Location = adjustLocation(contentLocation);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        ResponseInputStream<GetObjectResponse> getContentResponse = s3Adapter.getSyncS3Client().getObject(getObjectRequest, ResponseTransformer.toInputStream());
        return new BufferedInputStream(getContentResponse);
    }

    @Override
    public long streamContentToOutput(String contentLocation, OutputStream outputStream) {
        String s3Location = adjustLocation(contentLocation);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(s3Adapter.getBucket())
                .key(s3Location)
                .build();

        GetObjectResponse getContentResponse = s3Adapter.getSyncS3Client().getObject(getObjectRequest,
                ResponseTransformer.toOutputStream(outputStream));

        return getContentResponse.contentLength();
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
            s3Adapter.getSyncS3Client().putObject(putObjectRequest,
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
            s3Adapter.getSyncS3Client().deleteObject(deleteObjectRequest);
        } catch (Exception e) {
            throw new ContentException("Error deleting content at " + contentLocation, e);
        }
    }

}
