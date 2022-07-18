package org.janelia.jacsstorage.newclient;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * An object stored within a JADE StorageLocation.
 */
public class StorageObject {

    private StorageLocation location;
    private String relativePath;
    private String objectName;
    private final Long sizeBytes;
    private final boolean isCollection;

//    StorageObject(StorageLocation location, String relativePath, String objectName, Long sizeBytes, boolean isCollection) {
//        this.location = location;
//        this.relativePath = relativePath;
//        this.objectName = objectName;
//        this.sizeBytes = sizeBytes;
//        this.isCollection = isCollection;
//    }
//
//    StorageObject(StorageLocation location, String relativePath, StorageContentInfo storageContentInfo) {
//        this.location = location;
//        this.relativePath = relativePath;
//        this.objectName = Paths.get(relativePath).getFileName().toString();
//        this.sizeBytes = storageContentInfo.getSize();
//        this.isCollection = storageContentInfo.getRemoteInfo().isCollection();
//    }

    StorageObject(StorageLocation location, String relativePath, StorageEntryInfo storageEntryInfo) {
        this.location = location;
        this.relativePath = relativePath;
        Path filename = Paths.get(relativePath).getFileName();
        this.objectName = filename == null ? "" : filename.toString();
        this.sizeBytes = storageEntryInfo.getSize();
        this.isCollection = storageEntryInfo.isCollection();
    }

    /**
     * The actual JADE server where this storage object can be found.
     * @return
     */
    public StorageLocation getLocation() {
        return location;
    }

    /**
     * Path to the object relative to the StorageLocation.
     * @return
     */
    public String getRelativePath() {
        return relativePath;
    }

    /**
     * Absolute JADE path, including the location prefix.
     * @return
     */
    public String getAbsolutePath() {
        return location.getAbsolutePath(relativePath);
    }

    /**
     * Name of the object.
     * @return
     */
    public String getObjectName() {
        return objectName;
    }

    /**
     * Size in bytes of the object.
     * @return
     */
    public Long getSizeBytes() {
        return sizeBytes;
    }

    /**
     * Whether the object represents a collection (e.g. directory on disk) and can be listed.
     * @return
     */
    public boolean isCollection() {
        return isCollection;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("location", location)
                .append("relativePath", relativePath)
                .append("objectName", objectName)
                .append("sizeBytes", sizeBytes)
                .append("isCollection", isCollection)
                .toString();
    }
}