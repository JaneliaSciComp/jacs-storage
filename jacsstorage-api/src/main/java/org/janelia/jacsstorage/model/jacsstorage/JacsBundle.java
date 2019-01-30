package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@PersistenceInfo(storeName ="jacsBundle", label="JacsBundle")
public class JacsBundle extends AbstractEntity {

    private String name;
    private String ownerKey;
    private String path;
    private Set<String> readersKeys = new HashSet<>();
    private Set<String> writersKeys = new HashSet<>();
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

    public void setPath(String path) {
        this.path = path;
    }

    @JsonIgnore
    public Path getRealStoragePath() {
        String storageRootDir = null;
        if (storageVolume != null) {
            storageRootDir = storageVolume.evalStorageRootDir(asStorageContext());
        }
        if (StringUtils.isNotBlank(storageRootDir)) {
            return Paths.get(storageRootDir, path);
        } else {
            return Paths.get(path);
        }
    }

    @JsonIgnore
    public StoragePathURI getStorageURI() {
        if (storageVolume == null) {
            return null;
        } else if (StringUtils.isNotBlank(storageVolume.getStorageVirtualPath())) {
            return StoragePathURI.createPathURI(Paths.get(storageVolume.getStorageVirtualPath(), getId().toString()).toString());
        } else {
            // otherwise cannot build the storage path URI
            return StoragePathURI.createPathURI(null);
        }
    }

    public Set<String> getReadersKeys() {
        return readersKeys;
    }

    public void setReadersKeys(Set<String> readersKeys) {
        this.readersKeys = readersKeys;
    }

    public void addReadersKeys(Collection<String> readersKeys) {
        this.readersKeys.addAll(readersKeys);
    }

    public boolean hasReadPermissions(String readerKey) {
        return StringUtils.isNotBlank(readerKey) && (readerKey.equals(ownerKey) || readersKeys.contains(readerKey));
    }

    public Set<String> getWritersKeys() {
        return writersKeys;
    }

    public void setWritersKeys(Set<String> writersKeys) {
        this.writersKeys = writersKeys;
    }

    public void addWritersKeys(Collection<String> writersKeys) {
        this.writersKeys.addAll(writersKeys);
    }

    public boolean hasWritePermissions(String writerKey) {
        return StringUtils.isNotBlank(writerKey) && (writerKey.equals(ownerKey) || writersKeys.contains(writerKey));
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

    @JsonProperty
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

    public Map<String, Object> asStorageContext() {
        Map<String, Object> storageContext = new LinkedHashMap<>();
        addToContext("name", name, storageContext);
        addToContext("username", JacsSubjectHelper.getNameFromSubjectKey(ownerKey), storageContext);
        addToContext("createdBy", JacsSubjectHelper.getNameFromSubjectKey(createdBy), storageContext);
        addToContext("createDate", new SimpleDateFormat("yyyyMMddHHmmss").format(created), storageContext);
        metadata.forEach((k, v) -> addToContext(k, v, storageContext));
        return storageContext;
    }

    private void addToContext(String key, Object value, Map<String, Object> context) {
        if (value != null) {
            context.put(key, value);
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
