package org.janelia.jacsstorage.datarequest;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class StorageQuery {
    private Number id;
    private boolean shared;
    private boolean localToAnyHost;
    private String dataStoragePath;
    private List<String> storageHosts;
    private List<String> storageAgents;
    private String storageName;
    private String storagePathPrefix;
    private List<String> storageTags;
    private Long minAvailableSpaceInBytes;

    public Number getId() {
        return id;
    }

    public StorageQuery setId(Number id) {
        this.id = id;
        return this;
    }

    public boolean isShared() {
        return shared;
    }

    public StorageQuery setShared(boolean shared) {
        this.shared = shared;
        return this;
    }

    public boolean isLocalToAnyHost() {
        return localToAnyHost;
    }

    public StorageQuery setLocalToAnyHost(boolean localToAnyHost) {
        this.localToAnyHost = localToAnyHost;
        return this;
    }

    public String getDataStoragePath() {
        return dataStoragePath;
    }

    public StorageQuery setDataStoragePath(String dataStoragePath) {
        this.dataStoragePath = dataStoragePath;
        return this;
    }

    public List<String> getStorageHosts() {
        return storageHosts;
    }

    public StorageQuery setStorageHosts(List<String> storageHosts) {
        this.storageHosts = storageHosts;
        return this;
    }

    public StorageQuery addStorageHost(String storageHost) {
        if (storageHosts == null) {
            storageHosts = new ArrayList<>();
        }
        if (StringUtils.isNotBlank(storageHost)) storageHosts.add(storageHost);
        return this;
    }

    public List<String> getStorageAgents() {
        return storageAgents;
    }

    public StorageQuery setStorageAgents(List<String> storageAgents) {
        this.storageAgents = storageAgents;
        return this;
    }

    public StorageQuery addStorageAgents(String storageAgent) {
        if (storageAgents == null) {
            storageAgents = new ArrayList<>();
        }
        if (StringUtils.isNotBlank(storageAgent)) storageAgents.add(storageAgent);
        return this;
    }

    public String getStorageName() {
        return storageName;
    }

    public StorageQuery setStorageName(String storageName) {
        this.storageName = storageName;
        return this;
    }

    public String getStoragePathPrefix() {
        return storagePathPrefix;
    }

    public StorageQuery setStoragePathPrefix(String storagePathPrefix) {
        this.storagePathPrefix = storagePathPrefix;
        return this;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public StorageQuery setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
        return this;
    }

    public Long getMinAvailableSpaceInBytes() {
        return minAvailableSpaceInBytes;
    }

    public StorageQuery setMinAvailableSpaceInBytes(Long minAvailableSpaceInBytes) {
        this.minAvailableSpaceInBytes = minAvailableSpaceInBytes;
        return this;
    }

    public boolean hasMinAvailableSpaceInBytes() {
        return minAvailableSpaceInBytes != null && minAvailableSpaceInBytes > 0;
    }
}
