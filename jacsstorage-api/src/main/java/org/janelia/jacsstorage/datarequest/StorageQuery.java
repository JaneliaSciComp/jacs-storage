package org.janelia.jacsstorage.datarequest;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.ArrayList;
import java.util.List;

public class StorageQuery {
    private Number id;
    private boolean shared;
    private boolean localToAnyHost;
    private String dataStoragePath;
    private String accessibleOnHost;
    private List<String> storageHosts;
    private List<String> storageAgents;
    private String storageName;
    private String storageVirtualPath;
    private List<String> storageTags;
    private Long minAvailableSpaceInBytes;
    private boolean includeInactiveVolumes;

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

    public String getAccessibleOnHost() {
        return accessibleOnHost;
    }

    public StorageQuery setAccessibleOnHost(String accessibleOnHost) {
        this.accessibleOnHost = accessibleOnHost;
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

    public String getStorageVirtualPath() {
        return storageVirtualPath;
    }

    public StorageQuery setStorageVirtualPath(String storageVirtualPath) {
        this.storageVirtualPath = storageVirtualPath;
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

    public boolean isIncludeInactiveVolumes() {
        return includeInactiveVolumes;
    }

    public StorageQuery setIncludeInactiveVolumes(boolean includeInactiveVolumes) {
        this.includeInactiveVolumes = includeInactiveVolumes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        StorageQuery that = (StorageQuery) o;

        return new EqualsBuilder()
                .append(shared, that.shared)
                .append(localToAnyHost, that.localToAnyHost)
                .append(id, that.id)
                .append(dataStoragePath, that.dataStoragePath)
                .append(storageHosts, that.storageHosts)
                .append(storageAgents, that.storageAgents)
                .append(storageName, that.storageName)
                .append(storageVirtualPath, that.storageVirtualPath)
                .append(storageTags, that.storageTags)
                .append(minAvailableSpaceInBytes, that.minAvailableSpaceInBytes)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(id)
                .append(shared)
                .append(localToAnyHost)
                .append(dataStoragePath)
                .append(storageHosts)
                .append(storageAgents)
                .append(storageName)
                .append(storageVirtualPath)
                .append(storageTags)
                .append(minAvailableSpaceInBytes)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("shared", shared)
                .append("localToAnyHost", localToAnyHost)
                .append("dataStoragePath", dataStoragePath)
                .append("storageHosts", storageHosts)
                .append("storageAgents", storageAgents)
                .append("storageName", storageName)
                .append("storageVirtualPath", storageVirtualPath)
                .append("storageTags", storageTags)
                .append("minAvailableSpaceInBytes", minAvailableSpaceInBytes)
                .toString();
    }
}
