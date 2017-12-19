package org.janelia.jacsstorage.datarequest;

import java.util.Date;

public class DataNodeInfo {
    private String rootLocation;
    private String nodeRelativePath; // node path relative to the root
    private long size;
    private String mimeType;
    private boolean collectionFlag; // true if the node identifies a directory
    private Date creationTime;
    private Date lastModified;

    public String getRootLocation() {
        return rootLocation;
    }

    public void setRootLocation(String rootLocation) {
        this.rootLocation = rootLocation;
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
}
