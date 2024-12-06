package org.janelia.jacsstorage.clients.api;

import java.util.Collections;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A JADE storage location which is resolved to a single JADE worker. This object can be used to resolve any JADE
 * path with the prefix returned by getPathPrefix().
 */
public class StorageLocation {

    private String storageURL;
    private String storageType;
    private String pathPrefix;
    private String virtualPath;
    private JadeStorageAttributes storageAttributes;

    StorageLocation(String storageURL, String storageType, String pathPrefix, String virtualPath, JadeStorageAttributes storageAttributes) {
        this.storageURL = storageURL;
        this.storageType = storageType;
        this.pathPrefix = StringUtils.appendIfMissing(pathPrefix, "/");
        this.virtualPath = StringUtils.appendIfMissing(virtualPath, "/");
        this.storageAttributes = storageAttributes;
    }

    /**
     * Base URL to this storage location.
     *
     * @return
     */
    public String getStorageURL() {
        return storageURL;
    }

    public String getStorageType() {
        return storageType;
    }

    public boolean isFileSystemStorage() {
        return storageType == null || "FILE_SYSTEM".equals(storageType);
    }

    /**
     * Prefix for all paths in this StorageLocation.
     *
     * @return
     */
    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Given an absolute JADE path, generate the storage URL for the content.
     *
     * @param absolutePath
     * @return
     */
    public String getStorageURLForAbsolutePath(String absolutePath) {
        String relativePath = getRelativePath(absolutePath);
        return getStorageURLForRelativePath(relativePath);
    }

    /**
     * Given a JADE path relative to this StorageLocation, generate a storage URL for the content.
     *
     * @param relativePath
     * @return
     */
    public String getStorageURLForRelativePath(String relativePath) {
        return StringUtils.appendIfMissing(storageURL, "/") + "data_content/" + relativePath;
    }

    /**
     * Given an absolute JADE path, generate the path relative to this StorageLocation.
     *
     * @param absolutePath
     * @return
     */
    public String getRelativePath(String absolutePath) {
        if (StringUtils.startsWith(absolutePath, pathPrefix)) {
            return absolutePath.replaceFirst(pathPrefix, "");
        } else if (StringUtils.startsWith(absolutePath, virtualPath)) {
            return absolutePath.replaceFirst(virtualPath, "");
        } else if (StringUtils.equals(storageType, "S3")) {
            return absolutePath;
        } else if (!absolutePath.startsWith("/")) {
            throw new IllegalArgumentException("Not an absolute path: " + absolutePath);
        } else {
            throw new IllegalArgumentException("Given absolute path (" + absolutePath + ") does not exist in storage location with prefix " + pathPrefix);
        }
    }

    /**
     * Given a relative JADE path, generate the full absolute path.
     *
     * @param relativePath
     * @return
     */
    public String getAbsolutePath(String relativePath) {
        return StringUtils.isNotBlank(pathPrefix)
                ? pathPrefix + relativePath
                : relativePath;
    }

    /**
     * Given a relative JADE path, generate the full absolute virtual path.
     *
     * @param relativePath
     * @return
     */
    public String getVirtualPath(String relativePath) {
        return StringUtils.isNotBlank(virtualPath)
                ? virtualPath + relativePath
                : relativePath;
    }

    JadeStorageAttributes getStorageAttributes() {
        return storageAttributes;
    }

    Map<String, Object> getStorageAttributesAsMap() {
        return storageAttributes != null ? storageAttributes.getAsMap() : Collections.emptyMap();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("storageType", storageType)
                .append("pathPrefix", pathPrefix)
                .append("virtualPath", virtualPath)
                .append("storageURL", storageURL)
                .toString();
    }
}
