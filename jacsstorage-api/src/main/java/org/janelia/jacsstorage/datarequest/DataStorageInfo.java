package org.janelia.jacsstorage.datarequest;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataStorageInfo {
    private Number id;
    private String name;
    private String owner;
    private String path;
    private String permissions;
    private String storageHost;
    private int tcpPortNo;
    private String connectionURL;
    private JacsStorageFormat storageFormat;
    private Long requestedSpaceInBytes;
    private String checksum;
    private Map<String, Object> metadata = new LinkedHashMap<>();

    public static DataStorageInfo fromBundle(JacsBundle dataBundle) {
        DataStorageInfo dsi = new DataStorageInfo()
                .setId(dataBundle.getId())
                .setName(dataBundle.getName())
                .setOwner(dataBundle.getOwner())
                .setPath(dataBundle.getPath())
                .setStorageFormat(dataBundle.getStorageFormat())
                .setPermissions(dataBundle.getPermissions())
                .setRequestedSpaceInBytes(dataBundle.getUsedSpaceInBytes())
                .setChecksum(dataBundle.getChecksum())
                .addMetadata(dataBundle.getMetadata())
                ;
        dataBundle.getStorageVolume()
                .ifPresent(sv -> {
                    dsi.setStorageHost(sv.getStorageHost());
                    dsi.setTcpPortNo(sv.getStorageServiceTCPPortNo());
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

    public String getOwner() {
        return owner;
    }

    public DataStorageInfo setOwner(String owner) {
        this.owner = owner;
        return this;
    }

    public String getPath() {
        return path;
    }

    public DataStorageInfo setPath(String path) {
        this.path = path;
        return this;
    }

    public String getPermissions() {
        return permissions;
    }

    public DataStorageInfo setPermissions(String permissions) {
        this.permissions = permissions;
        return this;
    }

    public String getStorageHost() {
        return storageHost;
    }

    public DataStorageInfo setStorageHost(String storageHost) {
        this.storageHost = storageHost;
        return this;
    }

    public int getTcpPortNo() {
        return tcpPortNo;
    }

    public DataStorageInfo setTcpPortNo(int tcpPortNo) {
        this.tcpPortNo = tcpPortNo;
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
        JacsBundle dataBundle = new JacsBundle();
        dataBundle.setName(this.name);
        dataBundle.setOwner(this.owner);
        dataBundle.setStorageFormat(this.storageFormat);
        dataBundle.setPermissions(this.permissions);
        dataBundle.setUsedSpaceInBytes(this.requestedSpaceInBytes);
        dataBundle.setChecksum(this.checksum);
        dataBundle.addMetadataFields(this.metadata);
        return dataBundle;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("id", id)
                .append("name", name)
                .append("path", path)
                .append("storageHost", storageHost)
                .append("tcpPortNo", tcpPortNo)
                .append("connectionURL", connectionURL)
                .append("storageFormat", storageFormat)
                .toString();
    }
}
