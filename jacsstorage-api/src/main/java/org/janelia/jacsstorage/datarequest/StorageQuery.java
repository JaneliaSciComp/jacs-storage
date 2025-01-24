package org.janelia.jacsstorage.datarequest;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;

public class StorageQuery {
    private Number id;
    private boolean shared;
    private boolean localToAnyAgent;
    private String dataStoragePath;
    private String accessibleOnAgent;
    private List<String> storageAgentIds;
    private List<String> storageAgentURLs;
    private String storageName;
    private JacsStorageType storageType;
    private String storageVirtualPath;
    private List<String> storageTags;
    private Long minAvailableSpaceInBytes;
    private boolean includeInactiveVolumes;
    private boolean includeInaccessibleVolumes;

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

    public boolean isLocalToAnyAgent() {
        return localToAnyAgent;
    }

    public StorageQuery setLocalToAnyAgent(boolean localToAnyAgent) {
        this.localToAnyAgent = localToAnyAgent;
        return this;
    }

    public String getDataStoragePath() {
        return dataStoragePath;
    }

    public StorageQuery setDataStoragePath(String dataStoragePath) {
        this.dataStoragePath = dataStoragePath;
        return this;
    }

    public String getAccessibleOnAgent() {
        return accessibleOnAgent;
    }

    public StorageQuery setAccessibleOnAgent(String accessibleOnAgent) {
        this.accessibleOnAgent = accessibleOnAgent;
        return this;
    }

    public List<String> getStorageAgentIds() {
        return storageAgentIds;
    }

    public StorageQuery setStorageAgentIds(List<String> storageAgentIds) {
        this.storageAgentIds = storageAgentIds;
        return this;
    }

    public StorageQuery addStorageAgentId(String storageAgentId) {
        if (storageAgentIds == null) {
            storageAgentIds = new ArrayList<>();
        }
        if (StringUtils.isNotBlank(storageAgentId)) storageAgentIds.add(storageAgentId);
        return this;
    }

    public List<String> getStorageAgentURLs() {
        return storageAgentURLs;
    }

    public StorageQuery setStorageAgentURLs(List<String> storageAgentURLs) {
        this.storageAgentURLs = storageAgentURLs;
        return this;
    }

    public StorageQuery addStorageAgentURL(String storageAgentURL) {
        if (storageAgentURLs == null) {
            storageAgentURLs = new ArrayList<>();
        }
        if (StringUtils.isNotBlank(storageAgentURL)) storageAgentURLs.add(storageAgentURL);
        return this;
    }

    public String getStorageName() {
        return storageName;
    }

    public StorageQuery setStorageName(String storageName) {
        this.storageName = storageName;
        return this;
    }

    public JacsStorageType getStorageType() {
        return storageType;
    }

    public StorageQuery setStorageType(JacsStorageType storageType) {
        this.storageType = storageType;
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

    public boolean isIncludeInaccessibleVolumes() {
        return includeInaccessibleVolumes;
    }

    public StorageQuery setIncludeInaccessibleVolumes(boolean includeInaccessibleVolumes) {
        this.includeInaccessibleVolumes = includeInaccessibleVolumes;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        StorageQuery that = (StorageQuery) o;

        return new EqualsBuilder()
                .append(shared, that.shared)
                .append(localToAnyAgent, that.localToAnyAgent)
                .append(id, that.id)
                .append(dataStoragePath, that.dataStoragePath)
                .append(accessibleOnAgent, that.accessibleOnAgent)
                .append(storageAgentIds, that.storageAgentIds)
                .append(storageAgentURLs, that.storageAgentURLs)
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
                .append(localToAnyAgent)
                .append(dataStoragePath)
                .append(accessibleOnAgent)
                .append(storageAgentIds)
                .append(storageAgentURLs)
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
                .append("localToAnyAgent", localToAnyAgent)
                .append("dataStoragePath", dataStoragePath)
                .append("accessibleOnAgent", accessibleOnAgent)
                .append("storageAgentIds", storageAgentIds)
                .append("storageAgentURLs", storageAgentURLs)
                .append("storageName", storageName)
                .append("storageType", storageType)
                .append("storageVirtualPath", storageVirtualPath)
                .append("storageTags", storageTags)
                .append("minAvailableSpaceInBytes", minAvailableSpaceInBytes)
                .toString();
    }
}
