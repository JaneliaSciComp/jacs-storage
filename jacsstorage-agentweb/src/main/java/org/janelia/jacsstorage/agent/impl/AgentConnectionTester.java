package org.janelia.jacsstorage.agent.impl;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.resilience.ConnectionTester;

public class AgentConnectionTester implements ConnectionTester<AgentConnectionState> {

    private final JacsStorageAgent jacsStorageAgent;

    AgentConnectionTester(JacsStorageAgent jacsStorageAgent) {
        this.jacsStorageAgent = jacsStorageAgent;
    }

    @Override
    public AgentConnectionState testConnection(AgentConnectionState agentConnectionState) {
        if (StringUtils.isBlank(agentConnectionState.getMasterHttpURL())) {
            return new AgentConnectionState(
                    agentConnectionState.getStorageAgentId(),
                    agentConnectionState.getMasterHttpURL(),
                    agentConnectionState.getAgentHttpURL(),
                    ConnectionState.Status.OPEN,
                    agentConnectionState.getConnectionAttempts() + 1,
                    null);
        }
        if (!agentConnectionState.isConnected()) {
            StorageAgentInfo registeredAgentInfo = AgentConnectionHelper.registerAgent(agentConnectionState.getMasterHttpURL(), agentConnectionState.toStorageAgentInfo(jacsStorageAgent));
            if (registeredAgentInfo == null) {
                return new AgentConnectionState(
                        agentConnectionState.getStorageAgentId(),
                        agentConnectionState.getMasterHttpURL(),
                        agentConnectionState.getAgentHttpURL(),
                        ConnectionState.Status.OPEN,
                        agentConnectionState.getConnectionAttempts() + 1,
                        agentConnectionState.getRegisteredToken());
            } else {
                return new AgentConnectionState(
                        agentConnectionState.getStorageAgentId(),
                        agentConnectionState.getMasterHttpURL(),
                        agentConnectionState.getAgentHttpURL(),
                        ConnectionState.Status.CLOSED,
                        agentConnectionState.getConnectionAttempts() + 1,
                        registeredAgentInfo.getAgentToken());
            }
        } else {
            if (AgentConnectionHelper.findRegisteredAgent(agentConnectionState.getMasterHttpURL(), agentConnectionState.getAgentHttpURL()) == null) {
                return new AgentConnectionState(agentConnectionState.getStorageAgentId(),
                        agentConnectionState.getMasterHttpURL(),
                        agentConnectionState.getAgentHttpURL(),
                        ConnectionState.Status.OPEN,
                        1,
                        agentConnectionState.getRegisteredToken());
            } else {
                return agentConnectionState;
            }
        }
    }

}
