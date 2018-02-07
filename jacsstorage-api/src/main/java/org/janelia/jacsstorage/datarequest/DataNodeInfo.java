package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.math.BigInteger;
import java.util.Date;

public class DataNodeInfo {
    private String storageId;
    private String rootLocation;
    private String rootPrefix;
    private String nodeRelativePath; // node path relative to the root
    private long size;
    private String mimeType;
    private boolean collectionFlag; // true if the node identifies a directory
    private Date creationTime;
    private Date lastModified;

    public String getStorageId() {
        return storageId;
    }

    public void setStorageId(String storageId) {
        this.storageId = storageId;
    }

    @JsonIgnore
    public Number getNumericStorageId() {
        return new BigInteger(storageId);
    }

    public void setNumericStorageId(Number storageId) {
        this.storageId = storageId.toString();
    }

    public String getRootLocation() {
        return rootLocation;
    }

    public void setRootLocation(String rootLocation) {
        this.rootLocation = rootLocation;
    }

    public String getRootPrefix() {
        return rootPrefix;
    }

    public void setRootPrefix(String rootPrefix) {
        this.rootPrefix = rootPrefix;
    }

    public String getNodeRelativePath() {
        return nodeRelativePath;
    }

    public void setNodeRelativePath(String nodeRelativePath) {
        this.nodeRelativePath = nodeRelativePath;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getMimeType() {
        return mimeType;
    }

    public void setMimeType(String mimeType) {
        this.mimeType = mimeType;
    }

    public boolean isCollectionFlag() {
        return collectionFlag;
    }

    public void setCollectionFlag(boolean collectionFlag) {
        this.collectionFlag = collectionFlag;
    }

    public Date getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Date creationTime) {
        this.creationTime = creationTime;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("rootLocation", rootLocation)
                .append("rootPrefix", rootPrefix)
                .append("nodeRelativePath", nodeRelativePath)
                .append("size", size)
                .append("mimeType", mimeType)
                .append("collectionFlag", collectionFlag)
                .append("creationTime", creationTime)
                .append("lastModified", lastModified)
                .toString();
    }
}
