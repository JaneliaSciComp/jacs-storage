package org.janelia.jacsstorage.agent.impl;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.resilience.ConnectionState;

public class AgentConnectionState implements ConnectionState {

    private final String storageAgentId;
    private final String masterHttpURL;
    private final String agentHttpURL;
    private Status connectStatus;
    private int connectionAttempts;
    private String registeredToken;

    AgentConnectionState(String storageAgentId,
                         String masterHttpURL,
                         String agentHttpURL,
                         Status connectStatus,
                         int connectionAttempts,
                         String registeredToken) {
        this.storageAgentId = storageAgentId;
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

    String getStorageAgentId() {
        return storageAgentId;
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

    StorageAgentInfo toStorageAgentInfo(JacsStorageAgent jacsStorageAgent) {
        StorageAgentInfo agentInfo = new StorageAgentInfo(storageAgentId, agentHttpURL, jacsStorageAgent.getServedVolumes(), jacsStorageAgent.getUnavailableVolumeIds());
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
