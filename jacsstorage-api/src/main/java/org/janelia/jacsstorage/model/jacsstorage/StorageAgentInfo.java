package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class StorageAgentInfo {
    public static final String OVERFLOW_AGENT = "OVERFLOW_AGENT";

    private final String location; // agent running location
    private final String agentURL;
    private final String connectionInfo;
    private final String storagePath;
    private Long storageSpaceAvailableInBytes;
    private String connectionStatus;
    private String agentToken;

    @JsonCreator
    public StorageAgentInfo(@JsonProperty("location") String location,
                            @JsonProperty("agentURL") String agentURL,
                            @JsonProperty("connectionInfo") String connectionInfo,
                            @JsonProperty("storagePath") String storagePath) {
        this.location = location;
        this.agentURL = agentURL;
        this.connectionInfo = connectionInfo;
        this.storagePath = storagePath;
    }

    public String getLocation() {
        return location;
    }

    public String getAgentURL() {
        return agentURL;
    }

    public String getConnectionInfo() {
        return connectionInfo;
    }

    public String getStoragePath() {
        return storagePath;
    }

    public Long getStorageSpaceAvailableInBytes() {
        return storageSpaceAvailableInBytes;
    }

    public void setStorageSpaceAvailableInBytes(Long storageSpaceAvailableInBytes) {
        this.storageSpaceAvailableInBytes = storageSpaceAvailableInBytes;
    }

    @JsonIgnore
    public String getRegisteredLocation() {
        return StringUtils.defaultIfBlank(location, connectionInfo);
    }

    public String getConnectionStatus() {
        return connectionStatus;
    }

    public void setConnectionStatus(String connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public String getAgentToken() {
        return agentToken;
    }

    public void setAgentToken(String agentToken) {
        this.agentToken = agentToken;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("location", location)
                .append("agentToken", agentToken)
                .append("agentURL", agentURL)
                .append("connectionInfo", connectionInfo)
                .append("storagePath", storagePath)
                .toString();
    }

}
