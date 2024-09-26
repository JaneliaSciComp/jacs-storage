package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.OriginalStoragePathURI;

import java.net.URI;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ApiModel(
        value = "Storage information"
)
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
    private Map<String, Object> metadata = new LinkedHashMap<>();

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
                .addMetadata(dataBundle.getMetadata())
                ;
        dataBundle.getStorageVolume()
                .ifPresent(sv -> {
                    JADEStorageURI volumeRootStorageURI = sv.getVolumeStorageRootURI();
                    dsi.setStorageAgentId(sv.getStorageAgentId());
                    dsi.setStorageTags(sv.getStorageTags());
                    dsi.setStorageRootDir(sv.evalStorageRootDir(dataBundle.asStorageContext()));
                    dsi.setDataVirtualPath(Paths.get(sv.getStorageVirtualPath(), dataBundle.getId().toString()).toString());
                    dsi.setStorageRootPathURI(volumeRootStorageURI != null ? volumeRootStorageURI.getJadeStorage() : null);
                    dsi.setConnectionURL(sv.getStorageServiceURL());
                });
        return dsi;
    }

    @ApiModelProperty(
            value = "storage ID"
    )
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

    @ApiModelProperty(
            value = "storage name",
            notes = "this value must be unique for a user"
    )
    public String getName() {
        return name;
    }

    public DataStorageInfo setName(String name) {
        this.name = name;
        return this;
    }

    @ApiModelProperty(
            value = "storage owner key compatible with JACS subject key format - 'user:username'"
    )
    public String getOwnerKey() {
        return ownerKey;
    }

    public DataStorageInfo setOwnerKey(String ownerKey) {
        this.ownerKey = ownerKey;
        return this;
    }

    @ApiModelProperty(
            value = "path relative to the storage volume root directory"
    )
    public String getPath() {
        return path;
    }

    public DataStorageInfo setPath(String path) {
        this.path = path;
        return this;
    }

    @ApiModelProperty(
            value = "bundle absolute virtual path"
    )
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

    @ApiModelProperty(
            value = "list of subject keys that can read this storage",
            notes = "If no readers are specified the only ones who can access the storage are the owner and the admin users"
    )
    public Set<String> getReadersKeys() {
        return readersKeys;
    }

    public DataStorageInfo setReadersKeys(Set<String> readersKeys) {
        this.readersKeys = readersKeys;
        return this;
    }

    @ApiModelProperty(
            value = "list of subject keys that can write to this storage",
            notes = "If no writers are specified the only ones who can access the storage are the owner and the admin users"
    )
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

    @ApiModelProperty(
            value = "real directory path for this storage on the storage server"
    )
    public String getStorageRootDir() {
        return storageRootDir;
    }

    public DataStorageInfo setStorageRootDir(String storageRootDir) {
        this.storageRootDir = storageRootDir;
        return this;
    }

    @ApiModelProperty(
            value = "storage host"
    )
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

    @ApiModelProperty(
            value = "storage agent URL"
    )
    public String getConnectionURL() {
        return connectionURL;
    }

    public DataStorageInfo setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
        return this;
    }

    @ApiModelProperty(
            value = "storage format value",
            allowableValues = "DATA_DIRECTORY, ARCHIVE_DATA_FILE, SINGLE_DATA_FILE",
            notes = "specifies how should the data be stored - directory, tar archive or single file (this only supports one file)"
    )
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
                .storageAgentId(this.storageAgentId)
                .storageRootPath(this.storageRootDir)
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
