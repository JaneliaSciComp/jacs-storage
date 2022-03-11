package org.janelia.jacsstorage.newclient;

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

    StorageLocation(String storageURL, String pathPrefix) {
        this.storageURL = storageURL;
        this.pathPrefix = StringUtils.appendIfMissing(pathPrefix, "/");
    }

    StorageLocation(StorageEntryInfo storageEntryInfo) {
        this(storageEntryInfo.getStorageURL(), storageEntryInfo.getStorageRootLocation());
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
        // Escape forward slashes so that the path can be searched using regex
        // Remove path prefix to generate a path relative to this storage location
        String regex = "^" + pathPrefix;
        if (regex.endsWith("/")) {
            regex += "?";
        }
        return absolutePath.replaceFirst(regex, "");
    }

    /**
     * Given a relative JADE path, generate the full absolute path.
     * @param relativePath
     * @return
     */
    public String getAbsolutePath(String relativePath) {
        return pathPrefix + relativePath;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE)
                .append("pathPrefix", pathPrefix)
                .append("storageURL", storageURL)
                .toString();
    }
}
