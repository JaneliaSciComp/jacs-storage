package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;

import java.math.BigInteger;
import java.util.Date;

public class DataNodeInfo {
    private String storageId;
    private String storageRootLocation;
    private StoragePathURI storageRootPathURI;
    private String nodeAccessURL;
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

    public String getStorageRootLocation() {
        return storageRootLocation;
    }

    public void setStorageRootLocation(String storageRootLocation) {
        this.storageRootLocation = storageRootLocation;
    }

    public StoragePathURI getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public void setStorageRootPathURI(StoragePathURI storageRootPathURI) {
        this.storageRootPathURI = storageRootPathURI;
    }

    public String getNodeAccessURL() {
        return nodeAccessURL;
    }

    public void setNodeAccessURL(String nodeAccessURL) {
        this.nodeAccessURL = nodeAccessURL;
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
                .append("storageRootLocation", storageRootLocation)
                .append("storageRootPathURI", storageRootPathURI)
                .append("nodeRelativePath", nodeRelativePath)
                .append("size", size)
                .append("mimeType", mimeType)
                .append("collectionFlag", collectionFlag)
                .append("creationTime", creationTime)
                .append("lastModified", lastModified)
                .toString();
    }
}
