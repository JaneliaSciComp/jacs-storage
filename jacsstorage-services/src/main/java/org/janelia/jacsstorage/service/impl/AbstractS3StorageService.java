package org.janelia.jacsstorage.service.impl;

import java.time.Instant;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.jacsstorage.service.s3.S3Adapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.S3Object;

abstract class AbstractS3StorageService implements ContentStorageService {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractS3StorageService.class);

    final S3Adapter s3Adapter;

    AbstractS3StorageService(S3Adapter s3Adapter) {
        this.s3Adapter = s3Adapter;
    }

    public List<ContentNode> listContentNodes(String contentLocation, ContentAccessParams contentAccessParams) {
        long startTime = System.currentTimeMillis();
        try {
            String s3Location = adjustLocation(contentLocation);
            LOG.debug("List content {} with {}", s3Location, contentAccessParams);

            if (contentAccessParams.isDirectoriesOnly()) {
                return listPrefixNodes(s3Location, contentAccessParams);
            } else {
                return listObjectNodes(s3Location, contentAccessParams);
            }
        } finally {
            LOG.debug("List content {} with {} - {} secs", contentLocation, contentAccessParams, (System.currentTimeMillis() - startTime) / 1000.);
        }
    }

    abstract List<ContentNode> listPrefixNodes(String s3Location, ContentAccessParams contentAccessParams);

    abstract List<ContentNode> listObjectNodes(String s3Location, ContentAccessParams contentAccessParams);

    ContentNode createObjectNode(String key, Long size, Instant lastModified) {
        try {
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
                    .setSize(size != null ? size : 0L)
                    .setLastModified(new Date(lastModified.toEpochMilli()))
                    .setCollection(false)
                    ;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    ContentNode createObjectNode(S3Object s3Object) {
        return createObjectNode(s3Object.key(), s3Object.size(), s3Object.lastModified());
    }

    ContentNode createPrefixNode(String s3Prefix) {
        try {
            String key = StringUtils.removeEnd(s3Prefix, "/");
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
    public StorageCapacity getStorageCapacity(String contentLocation) {
        return new StorageCapacity(-1L, -1L); // don't know how to calculate it
    }

    String adjustLocation(String contentLocation) {
        return normalizedNonBlank(StringUtils.removeStart(contentLocation, '/'));
    }

    private String normalizedNonBlank(String s) {
        return StringUtils.isBlank(s) ? "" : s;
    }

    int getPathDepth(String basePath, String p) {
        String relativePrefix = StringUtils.removeEnd(
                StringUtils.removeStart(p.substring(basePath.length()), '/'),
                "/");
        return StringUtils.isEmpty(relativePrefix) ? 0 : StringUtils.countMatches(relativePrefix, '/') + 1;
    }

}
