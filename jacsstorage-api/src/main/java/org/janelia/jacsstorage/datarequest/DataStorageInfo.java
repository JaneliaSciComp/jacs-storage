package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataStorageInfo {
    private Number id;
    private String name;
    private String ownerKey;
    private String path;
    private Set<String> readersKeys = new HashSet<>();
    private Set<String> writersKeys = new HashSet<>();
    private String storageRootPrefixDir;
    private String storageRootRealDir;
    private String storageHost;
    private List<String> storageTags;
    private String connectionURL;
    private JacsStorageFormat storageFormat;
    private Long requestedSpaceInBytes;
    private String checksum;
    private Map<String, Object> metadata = new LinkedHashMap<>();

    public static DataStorageInfo fromBundle(JacsBundle dataBundle) {
        DataStorageInfo dsi = new DataStorageInfo()
                .setId(dataBundle.getId())
                .setName(dataBundle.getName())
                .setOwnerKey(dataBundle.getOwnerKey())
                .setPath(dataBundle.getPath())
                .setStorageFormat(dataBundle.getStorageFormat())
                .setReadersKeys(dataBundle.getReadersKeys())
                .setWritersKeys(dataBundle.getWritersKeys())
                .setRequestedSpaceInBytes(dataBundle.getUsedSpaceInBytes())
                .setChecksum(dataBundle.getChecksum())
                .addMetadata(dataBundle.getMetadata())
                ;
        dataBundle.getStorageVolume()
                .ifPresent(sv -> {
                    dsi.setStorageHost(sv.getStorageHost());
                    dsi.setStorageTags(sv.getStorageTags());
                    dsi.setStorageRootRealDir(sv.getStorageRootDir());
                    dsi.setStorageRootPrefixDir(sv.getStoragePathPrefix());
                    dsi.setConnectionURL(sv.getStorageServiceURL());
                });
        return dsi;
    }

    public Number getId() {
        return id;
    }

    public DataStorageInfo setId(Number id) {
        this.id = id;
        return this;
    }

    public boolean hasId() {
        return id != null && id.longValue() != 0L;
    }

    public String getName() {
        return name;
    }

    public DataStorageInfo setName(String name) {
        this.name = name;
        return this;
    }

    public String getOwnerKey() {
        return ownerKey;
    }

    public DataStorageInfo setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
        return this;
    }

    public String getPath() {
        return path;
    }

    public DataStorageInfo setPath(String path) {
        this.path = path;
        return this;
    }

    @JsonIgnore
    public String getDataStoragePath() {
        return StringUtils.isNotBlank(storageRootRealDir)
                ? Paths.get(storageRootRealDir, StringUtils.defaultIfBlank(path, "")).toString()
                : (StringUtils.isBlank(path) ? "" : Paths.get(path).toString());
    }

    public Set<String> getReadersKeys() {
        return readersKeys;
    }

    public DataStorageInfo setReadersKeys(Set<String> readersKeys) {
        this.readersKeys = readersKeys;
        return this;
    }

    public Set<String> getWritersKeys() {
        return writersKeys;
    }

    public DataStorageInfo setWritersKeys(Set<String> writersKeys) {
        this.writersKeys = writersKeys;
        return this;
    }

    public String getStorageRootPrefixDir() {
        return storageRootPrefixDir;
    }

    public DataStorageInfo setStorageRootPrefixDir(String storageRootPrefixDir) {
        this.storageRootPrefixDir = storageRootPrefixDir;
        return this;
    }

    public String getStorageRootRealDir() {
        return storageRootRealDir;
    }

    public DataStorageInfo setStorageRootRealDir(String storageRootRealDir) {
        this.storageRootRealDir = storageRootRealDir;
        return this;
    }

    public String getStorageHost() {
        return storageHost;
    }

    public DataStorageInfo setStorageHost(String storageHost) {
        this.storageHost = storageHost;
        return this;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public DataStorageInfo setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
        return this;
    }

    public String getConnectionURL() {
        return connectionURL;
    }

    public DataStorageInfo setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
        return this;
    }

    public JacsStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public DataStorageInfo setStorageFormat(JacsStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
        return this;
    }

    public Long getRequestedSpaceInBytes() {
        return requestedSpaceInBytes;
    }

    public DataStorageInfo setRequestedSpaceInBytes(Long requestedSpaceInBytes) {
        this.requestedSpaceInBytes = requestedSpaceInBytes;
        return this;
    }

    public String getChecksum() {
        return checksum;
    }

    public DataStorageInfo setChecksum(String checksum) {
        this.checksum = checksum;
        return this;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public DataStorageInfo setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
        return this;
    }

    public DataStorageInfo addMetadata(Map<String, Object> metadata) {
        this.metadata.putAll(metadata);
        return this;
    }

    public JacsBundle asDataBundle() {
        return new JacsBundleBuilder()
                .name(this.name)
                .ownerKey(this.ownerKey)
                .storageFormat(this.storageFormat)
                .readersKeys(this.readersKeys)
                .writersKeys(this.writersKeys)
                .usedSpaceInBytes(this.requestedSpaceInBytes)
                .checksum(this.checksum)
                .metadata(this.metadata)
                .storageHost(this.storageHost)
                .storageTagsAsList(this.storageTags)
                .build();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("path", path)
                .append("storageHost", storageHost)
                .append("connectionURL", connectionURL)
                .append("storageFormat", storageFormat)
                .toString();
    }
}
