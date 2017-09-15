package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.support.MongoMapping;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

@MongoMapping(collectionName="jacsBundle", label="JacsBundle")
public class JacsBundle extends AbstractEntity {

    private String name;
    private String owner;
    private String path;
    private String permissions;
    private JacsStorageFormat storageFormat;
    private Long usedSpaceInKB;
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

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getPath() {
        return path;
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

    public Long getUsedSpaceInKB() {
        return usedSpaceInKB;
    }

    public void setUsedSpaceInKB(Long usedSpaceInKB) {
        this.usedSpaceInKB = usedSpaceInKB;
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

    public void addMetadataField(String name, Object value) {
        metadata.put(name, value);
    }

    public void removeMetadataField(String name) {
        metadata.remove(name);
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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("name", name)
                .append("path", path)
                .append("storageFormat", storageFormat)
                .toString();
    }
}
