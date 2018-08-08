package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;

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
    private Set<String> readersKeys = new HashSet<>();
    private Set<String> writersKeys = new HashSet<>();
    private StoragePathURI storageRootURI;
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
                    dsi.setStorageHost(sv.getStorageHost());
                    dsi.setStorageTags(sv.getStorageTags());
                    dsi.setStorageRootRealDir(sv.getStorageRootDir());
                    dsi.setStorageRootURI(sv.getStorageURI());
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

    @JsonIgnore
    public String getDataStoragePath() {
        return StringUtils.isNotBlank(storageRootRealDir)
                ? Paths.get(storageRootRealDir, StringUtils.defaultIfBlank(path, "")).toString()
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

    public StoragePathURI getStorageRootURI() {
        return storageRootURI;
    }

    public DataStorageInfo setStorageRootURI(StoragePathURI storageRootURI) {
        this.storageRootURI = storageRootURI;
        return this;
    }

    @ApiModelProperty(
            value = "real directory path for this storage on the storage server"
    )
    public String getStorageRootRealDir() {
        return storageRootRealDir;
    }

    public DataStorageInfo setStorageRootRealDir(String storageRootRealDir) {
        this.storageRootRealDir = storageRootRealDir;
        return this;
    }

    @ApiModelProperty(
            value = "storage host"
    )
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
