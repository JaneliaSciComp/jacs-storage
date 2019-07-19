package org.janelia.jacsstorage.datarequest;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public class StorageAgentInfo {

    private final String agentHost;
    private final String agentAccessURL;
    private String connectionStatus;
    private String agentToken;
    private Set<String> servedVolumes;

    @JsonCreator
    public StorageAgentInfo(@JsonProperty("agentHost") String agentHost,
                            @JsonProperty("agentAccessURL") String agentAccessURL,
                            @JsonProperty("servedVolumes") Set<String> servedVolumes) {
        this.agentHost = agentHost;
        this.agentAccessURL = agentAccessURL;
        this.servedVolumes = servedVolumes;
    }

    public String getAgentHost() {
        return agentHost;
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
            return agentHost.equals(storageVolume.getStorageHost());
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentHost", agentHost)
                .append("agentAccessURL", agentAccessURL)
                .append("agentToken", agentToken)
                .append("connectionStatus", connectionStatus)
                .toString();
    }

}
