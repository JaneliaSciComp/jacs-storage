package org.janelia.jacsstorage.datarequest;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public class StorageAgentInfo {

    private final String agentId;
    private final String agentAccessURL;
    private String connectionStatus;
    private String agentToken;
    private Set<String> servedVolumes;

    @JsonCreator
    public StorageAgentInfo(@JsonProperty("agentId") String agentId,
                            @JsonProperty("agentAccessURL") String agentAccessURL,
                            @JsonProperty("servedVolumes") Set<String> servedVolumes) {
        this.agentId = agentId;
        this.agentAccessURL = agentAccessURL;
        this.servedVolumes = servedVolumes;
    }

    public String getAgentId() {
        return agentId;
    }

    public String getAgentAccessURL() {
        return agentAccessURL;
    }

    public Set<String> getServedVolumes() {
        return servedVolumes;
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

    public boolean canServe(JacsStorageVolume storageVolume) {
        if (storageVolume.isShared()) {
            return servedVolumes.contains("*") || servedVolumes.contains(storageVolume.getName());
        } else {
            return agentId.equals(storageVolume.getStorageAgentId());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentId", agentId)
                .append("agentAccessURL", agentAccessURL)
                .append("agentToken", agentToken)
                .append("connectionStatus", connectionStatus)
                .toString();
    }

}
