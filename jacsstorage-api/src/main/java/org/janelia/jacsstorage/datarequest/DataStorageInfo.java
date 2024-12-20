package org.janelia.jacsstorage.datarequest;

import java.net.URI;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.media.SchemaProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

@Schema(description = "Storage information")
public class DataStorageInfo {
    private String id;
    private String name;
    private String ownerKey;
    private String path;
    private String dataVirtualPath;
    private Set<String> readersKeys = new HashSet<>();
    private Set<String> writersKeys = new HashSet<>();
    private String storageRootPathURI;
    private String storageRootDir;
    private String storageAgentId;
    private List<String> storageTags;
    private String connectionURL;
    private JacsStorageFormat storageFormat;
    private Long requestedSpaceInBytes;
    private String checksum;

    public static DataStorageInfo fromBundle(JacsBundle dataBundle) {
        DataStorageInfo dsi = new DataStorageInfo()
                .setNumericId(dataBundle.getId())
                .setName(dataBundle.getName())
                .setOwnerKey(dataBundle.getOwnerKey())
                .setPath(dataBundle.getPath())
                .setStorageFormat(dataBundle.getStorageFormat())
                .setReadersKeys(dataBundle.getReadersKeys())
                .setWritersKeys(dataBundle.getWritersKeys())
                .setRequestedSpaceInBytes(dataBundle.getUsedSpaceInBytes())
                .setChecksum(dataBundle.getChecksum())
                ;
        dataBundle.getStorageVolume()
                .ifPresent(sv -> {
                    JADEStorageURI volumeRootStorageURI = sv.getVolumeStorageRootURI();
                    dsi.setStorageAgentId(sv.getStorageAgentId());
                    dsi.setStorageTags(sv.getStorageTags());
                    dsi.setStorageRootDir(sv.evalStorageRoot(dataBundle.asStorageContext()));
                    dsi.setDataVirtualPath(Paths.get(sv.getStorageVirtualPath(), dataBundle.getId().toString()).toString());
                    dsi.setStorageRootPathURI(volumeRootStorageURI != null ? volumeRootStorageURI.getJadeStorage() : null);
                    dsi.setConnectionURL(sv.getStorageServiceURL());
                });
        return dsi;
    }

    @Schema(description = "storage ID")
    public String getId() {
        return id;
    }

    public DataStorageInfo setId(String id) {
        this.id = id;
        return this;
    }

    @JsonIgnore
    public Number getNumericId() {
        return hasId() ? Long.valueOf(id) : null;
    }

    public DataStorageInfo setNumericId(Number id) {
        this.id = id != null ? id.toString() : null;
        return this;
    }

    public boolean hasId() {
        return StringUtils.isNotBlank(id) && !"0".equals(id);
    }

    @Schema(description = "storage name")
    public String getName() {
        return name;
    }

    public DataStorageInfo setName(String name) {
        this.name = name;
        return this;
    }

    @Schema(description = "storage owner key compatible with JACS subject key format - 'user:username'")
    public String getOwnerKey() {
        return ownerKey;
    }

    public DataStorageInfo setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
        return this;
    }

    @Schema(description = "path relative to the storage volume root directory")
    public String getPath() {
        return path;
    }

    public DataStorageInfo setPath(String path) {
        this.path = path;
        return this;
    }

    @Schema(description = "bundle absolute virtual path")
    public String getDataVirtualPath() {
        return dataVirtualPath;
    }

    public DataStorageInfo setDataVirtualPath(String dataVirtualPath) {
        this.dataVirtualPath = dataVirtualPath;
        return this;
    }

    @JsonIgnore
    public String getDataStoragePath() {
        return StringUtils.isNotBlank(storageRootDir)
                ? Paths.get(storageRootDir, StringUtils.defaultIfBlank(path, "")).toString()
                : (StringUtils.isBlank(path) ? "" : Paths.get(path).toString());
    }

    @Schema(description = "list of subject keys that can read this storage")
    public Set<String> getReadersKeys() {
        return readersKeys;
    }

    public DataStorageInfo setReadersKeys(Set<String> readersKeys) {
        this.readersKeys = readersKeys;
        return this;
    }

    @Schema(description = "list of subject keys that can write to this storage")
    public Set<String> getWritersKeys() {
        return writersKeys;
    }

    public DataStorageInfo setWritersKeys(Set<String> writersKeys) {
        this.writersKeys = writersKeys;
        return this;
    }

    public String getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public DataStorageInfo setStorageRootPathURI(String storageRootPathURI) {
        this.storageRootPathURI = storageRootPathURI;
        return this;
    }

    @JsonProperty
    public String getDataStorageURI() {
        String connectionUrl = StringUtils.appendIfMissing(StringUtils.defaultIfBlank(getConnectionURL(), "/"), "/");
        if (hasId()) {
            return URI.create(connectionUrl).resolve("agent_storage/").resolve(getId()).toString();
        } else {
            return URI.create(connectionUrl).resolve("agent_storage/").toString();
        }
    }

    @JsonIgnore
    public void setDataStorageURI(String dataStorageURI) {
    }

    @Schema(description = "real directory path for this storage on the storage server")
    public String getStorageRootDir() {
        return storageRootDir;
    }

    public DataStorageInfo setStorageRootDir(String storageRootDir) {
        this.storageRootDir = storageRootDir;
        return this;
    }

    @Schema(description = "storage host")
    public String getStorageAgentId() {
        return storageAgentId;
    }

    public DataStorageInfo setStorageAgentId(String storageAgentId) {
        this.storageAgentId = storageAgentId;
        return this;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public DataStorageInfo setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
        return this;
    }

    @Schema(description = "storage agent URL")
    public String getConnectionURL() {
        return connectionURL;
    }

    public DataStorageInfo setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
        return this;
    }

    @Schema(description = "storage format value")
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

    public JacsBundle asDataBundle() {
        return new JacsBundleBuilder()
                .name(this.name)
                .ownerKey(this.ownerKey)
                .storageFormat(this.storageFormat)
                .readersKeys(this.readersKeys)
                .writersKeys(this.writersKeys)
                .usedSpaceInBytes(this.requestedSpaceInBytes)
                .checksum(this.checksum)
                .storageAgentId(this.storageAgentId)
                .storageTagsAsList(this.storageTags)
                .build();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("path", path)
                .append("storageAgentId", storageAgentId)
                .append("connectionURL", connectionURL)
                .append("storageFormat", storageFormat)
                .toString();
    }
}
