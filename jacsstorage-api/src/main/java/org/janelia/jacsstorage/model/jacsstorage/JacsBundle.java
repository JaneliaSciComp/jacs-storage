package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@PersistenceInfo(storeName ="jacsBundle", label="JacsBundle")
public class JacsBundle extends AbstractEntity {

    private String name;
    private String ownerKey;
    private String path;
    private String permissions;
    private JacsStorageFormat storageFormat;
    private Long usedSpaceInBytes;
    private String checksum;
    private String createdBy;
    private Date created = new Date();
    private Date modified = new Date();
    private Map<String, Object> metadata = new LinkedHashMap<>();
    private Number storageVolumeId;
    @JsonIgnore
    private JacsStorageVolume storageVolume;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwnerKey() {
        return ownerKey;
    }

    public void setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
    }

    public String getPath() {
        return path;
    }

    @JsonIgnore
    public Path getRealStoragePath() {
        if (storageVolume == null) {
            return Paths.get(path);
        } else if (StringUtils.isNotBlank(storageVolume.getStorageRootDir())) {
            return Paths.get(storageVolume.getStorageRootDir(), path);
        } else {
            return Paths.get(path);
        }
    }

    @JsonIgnore
    public String getVirtualRoot() {
        if (storageVolume == null) {
            return null;
        } else if (StringUtils.isNotBlank(storageVolume.getStoragePathPrefix())) {
            return Paths.get(storageVolume.getStoragePathPrefix(), getId().toString()).toString();
        } else {
            return getId().toString();
        }
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPermissions() {
        return permissions;
    }

    public void setPermissions(String permissions) {
        this.permissions = permissions;
    }

    public JacsStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public void setStorageFormat(JacsStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
    }

    public Long getUsedSpaceInBytes() {
        return usedSpaceInBytes;
    }

    public void setUsedSpaceInBytes(Long usedSpaceInBytes) {
        this.usedSpaceInBytes = usedSpaceInBytes;
    }

    public boolean hasUsedSpaceSet() {
        return usedSpaceInBytes != null && usedSpaceInBytes != 0L;
    }

    public long size() {
        return usedSpaceInBytes != null ? usedSpaceInBytes : 0;
    }

    public String getChecksum() {
        return checksum;
    }

    public void setChecksum(String checksum) {
        this.checksum = checksum;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getModified() {
        return modified;
    }

    public void setModified(Date modified) {
        this.modified = modified;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public boolean hasMetadata() {
        return metadata.size() > 0;
    }

    public void addMetadataField(String name, Object value) {
        metadata.put(name, value);
    }

    public void removeMetadataField(String name) {
        metadata.remove(name);
    }

    public void addMetadataFields(Map<String, Object> metadataFields) {
        if (metadataFields != null) {
            this.metadata.putAll(metadataFields);
        }
    }

    public Number getStorageVolumeId() {
        return storageVolumeId;
    }

    public void setStorageVolumeId(Number storageVolumeId) {
        this.storageVolumeId = storageVolumeId;
    }

    @JsonIgnore
    public Optional<JacsStorageVolume> getStorageVolume() {
        return storageVolume != null ? Optional.of(storageVolume) : Optional.empty();
    }

    public Optional<JacsStorageVolume> setStorageVolume(JacsStorageVolume storageVolume) {
        this.storageVolume = storageVolume;
        return getStorageVolume();
    }

    public boolean hasStorageHost() {
        return storageVolume != null && StringUtils.isNotBlank(storageVolume.getStorageHost());
    }

    @JsonProperty("referencedVolumes")
    public void referencedVolumes(List<JacsStorageVolume> referencedVolumes) {
        if (referencedVolumes != null && referencedVolumes.size() > 0) {
            this.storageVolume = referencedVolumes.get(0);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("name", name)
                .append("path", path)
                .append("storageFormat", storageFormat)
                .append("storageVolumeId", storageVolumeId)
                .toString();
    }
}
