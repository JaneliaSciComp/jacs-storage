package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class StorageAgentInfo {
    private final String location; // agent running location
    private final String connectionInfo;
    private final String storagePath;
    private Long storageSpaceAvailableInMB;

    public StorageAgentInfo(String location, String connectionInfo, String storagePath) {
        this.location = location;
        this.connectionInfo = connectionInfo;
        this.storagePath = storagePath;
    }

    public String getLocation() {
        return location;
    }

    public String getConnectionInfo() {
        return connectionInfo;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public Long getStorageSpaceAvailableInMB() {
        return storageSpaceAvailableInMB;
    }

    public void setStorageSpaceAvailableInMB(Long storageSpaceAvailableInMB) {
        this.storageSpaceAvailableInMB = storageSpaceAvailableInMB;
    }

    @JsonIgnore
    public String getRegisteredLocation() {
        return StringUtils.defaultIfBlank(location, connectionInfo);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("location", location)
                .append("connectionInfo", connectionInfo)
                .append("storagePath", storagePath)
                .toString();
    }
}
