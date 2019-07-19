package org.janelia.jacsstorage.agent;

import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionState;

public class AgentConnectionState implements ConnectionState {

    private final String storageHost;
    private final String masterHttpURL;
    private final String agentHttpURL;
    private Status connectStatus;
    private int connectionAttempts;
    private String registeredToken;

    AgentConnectionState(String storageHost,
                         String masterHttpURL,
                         String agentHttpURL,
                         Status connectStatus,
                         int connectionAttempts,
                         String registeredToken) {
        this.storageHost = storageHost;
        this.masterHttpURL = masterHttpURL;
        this.agentHttpURL = agentHttpURL;
        this.connectStatus = connectStatus;
        this.connectionAttempts = connectionAttempts;
        this.registeredToken = registeredToken;
    }

    @Override
    public Status getConnectStatus() {
        return connectStatus;
    }

    @Override
    public void setConnectStatus(Status connectStatus) {
        this.connectStatus = connectStatus;
    }

    @Override
    public int getConnectionAttempts() {
        return connectionAttempts;
    }

    @Override
    public void setConnectionAttempts(int connectionAttempts) {
        this.connectionAttempts = connectionAttempts;
    }

    String getStorageHost() {
        return storageHost;
    }

    String getMasterHttpURL() {
        return masterHttpURL;
    }

    String getAgentHttpURL() {
        return agentHttpURL;
    }

    boolean isNotRegistered() {
        return StringUtils.isBlank(registeredToken);
    }

    @Override
    public boolean isConnected() {
        return StringUtils.isNotBlank(registeredToken) && connectStatus == Status.CLOSED;
    }

    String getRegisteredToken() {
        return registeredToken;
    }

    void setRegisteredToken(String registeredToken) {
        this.registeredToken = registeredToken;
    }

    StorageAgentInfo toStorageAgentInfo(Set<String> servedVolumes) {
        StorageAgentInfo agentInfo = new StorageAgentInfo(storageHost, agentHttpURL, servedVolumes);
        if (this.isConnected()) {
            agentInfo.setConnectionStatus("CONNECTED");
        } else {
            agentInfo.setConnectionStatus("DISCONNECTED");
        }
        return agentInfo;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("masterHttpURL", masterHttpURL)
                .append("connectStatus", connectStatus)
                .append("connectionAttempts", connectionAttempts)
                .append("registeredToken", registeredToken)
                .build();
    }
}
