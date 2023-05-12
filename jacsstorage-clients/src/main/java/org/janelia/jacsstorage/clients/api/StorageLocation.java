package org.janelia.jacsstorage.clients.api;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * A JADE storage location which is resolved to a single JADE worker. This object can be used to resolve any JADE
 * path with the prefix returned by getPathPrefix().
 */
public class StorageLocation {

    private String storageURL;
    private String pathPrefix;
    private String virtualPath;

    StorageLocation(String storageURL, String pathPrefix, String virtualPath) {
        this.storageURL = storageURL;
        this.pathPrefix = StringUtils.appendIfMissing(pathPrefix, "/");
        this.virtualPath = StringUtils.appendIfMissing(virtualPath, "/");
    }

    /**
     * Base URL to this storage location.
     * @return
     */
    public String getStorageURL() {
        return storageURL;
    }

    /**
     * Prefix for all paths in this StorageLocation.
     * @return
     */
    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Given an absolute JADE path, generate the storage URL for the content.
     * @param absolutePath
     * @return
     */
    public String getStorageURLForAbsolutePath(String absolutePath) {
        String relativePath = getRelativePath(absolutePath);
        return getStorageURLForRelativePath(relativePath);
    }

    /**
     * Given a JADE path relative to this StorageLocation, generate a storage URL for the content.
     * @param relativePath
     * @return
     */
    public String getStorageURLForRelativePath(String relativePath) {
        return StringUtils.appendIfMissing(storageURL, "/") + "data_content/" + relativePath;
    }

    /**
     * Given an absolute JADE path, generate the path relative to this StorageLocation.
     * @param absolutePath
     * @return
     */
    public String getRelativePath(String absolutePath) {
        if (absolutePath.startsWith(pathPrefix)) {
            return absolutePath.replaceFirst(pathPrefix, "");
        }
        else if (absolutePath.startsWith(virtualPath)) {
            return absolutePath.replaceFirst(virtualPath, "");
        }
        else if (!absolutePath.startsWith("/")) {
            throw new IllegalArgumentException("Not an absolute path: "+absolutePath);
        }
        else {
            throw new IllegalArgumentException("Given absolute path (" + absolutePath + ") does not exist in storage location with prefix " + pathPrefix);
        }
    }

    /**
     * Given a relative JADE path, generate the full absolute path.
     * @param relativePath
     * @return
     */
    public String getAbsolutePath(String relativePath) {
        return pathPrefix + relativePath;
    }

    /**
     * Given a relative JADE path, generate the full absolute virtual path.
     * @param relativePath
     * @return
     */
    public String getVirtualPath(String relativePath) {
        return virtualPath + relativePath;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("pathPrefix", pathPrefix)
                .append("virtualPath", virtualPath)
                .append("storageURL", storageURL)
                .toString();
    }
}
