package org.janelia.jacsstorage.agent;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitTester;

public class AgentConnectionTester implements CircuitTester<AgentState> {

    @Override
    public boolean testConnection(AgentState agentState) {
        if (StringUtils.isBlank(agentState.getMasterURL())) {
            return false;
        }
        if (!agentState.isRegistered()) {
            StorageAgentInfo registeredAgentInfo = AgentConnectionHelper.registerAgent(agentState.getMasterURL(), agentState.getLocalAgentInfo());
            if (registeredAgentInfo == null) {
                return false;
            } else {
                agentState.setRegisteredToken(registeredAgentInfo.getAgentToken());
                return true;
            }
        } else {
            if (AgentConnectionHelper.findRegisteredAgent(agentState.getMasterURL(), agentState.getAgentLocation()) == null) {
                agentState.setRegisteredToken(null);
                return false;
            } else {
                return true;
            }
        }
    }

}
