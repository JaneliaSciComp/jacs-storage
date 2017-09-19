package org.janelia.jacsstorage.datarequest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.util.LinkedHashMap;
import java.util.Map;

public class DataStorageInfo {
    private String name;
    private String owner;
    private String path;
    private String permissions;
    private String connectionInfo;
    private JacsStorageFormat storageFormat;
    private Long requestedSpaceInKB;
    private Map<String, Object> metadata = new LinkedHashMap<>();

    public static DataStorageInfo fromBundle(JacsBundle dataBundle) {
        return new DataStorageInfo()
                .setName(dataBundle.getName())
                .setOwner(dataBundle.getOwner())
                .setStorageFormat(dataBundle.getStorageFormat())
                .setPermissions(dataBundle.getPermissions())
                .setRequestedSpaceInKB(dataBundle.getUsedSpaceInKB())
                .addMetadata(dataBundle.getMetadata())
                .setConnectionInfo(dataBundle.getConnectionInfo());
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

    public String getConnectionInfo() {
        return connectionInfo;
    }

    public DataStorageInfo setConnectionInfo(String connectionInfo) {
        this.connectionInfo = connectionInfo;
        return this;
    }

    public JacsStorageFormat getStorageFormat() {
        return storageFormat;
    }

    public DataStorageInfo setStorageFormat(JacsStorageFormat storageFormat) {
        this.storageFormat = storageFormat;
        return this;
    }

    public Long getRequestedSpaceInKB() {
        return requestedSpaceInKB;
    }

    public DataStorageInfo setRequestedSpaceInKB(Long requestedSpaceInKB) {
        this.requestedSpaceInKB = requestedSpaceInKB;
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

    public String getConnectionHost() {
        if (StringUtils.isNotBlank(connectionInfo)) {
            int separatorIndex = connectionInfo.indexOf(':');
            if (separatorIndex != -1) {
                return connectionInfo.substring(0, separatorIndex).trim();
            } else {
                return connectionInfo;
            }
        } else {
            return null;
        }
    }

    public int getConnectionPort() {
        if (StringUtils.isNotBlank(connectionInfo)) {
            int separatorIndex = connectionInfo.indexOf(':');
            if (separatorIndex != -1) {
                return Integer.parseInt(connectionInfo.substring(separatorIndex + 1).trim());
            } else {
                return -1;
            }
        } else {
            return -1;
        }
    }

    public JacsBundle asDataBundle() {
        JacsBundle dataBundle = new JacsBundle();
        dataBundle.setName(this.name);
        dataBundle.setOwner(this.owner);
        dataBundle.setStorageFormat(this.storageFormat);
        dataBundle.setPermissions(this.permissions);
        dataBundle.setUsedSpaceInKB(this.requestedSpaceInKB);
        dataBundle.addMetadataFields(this.metadata);
        return dataBundle;
    }
}
