package org.janelia.jacsstorage.agent;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.resilience.CircuitTester;

public class AgentConnectionTester implements CircuitTester<AgentState> {

    @Override
    public boolean testConnection(AgentState agentState) {
        if (StringUtils.isBlank(agentState.getMasterURL())) {
            return false;
        }
        if (!agentState.isRegistered()) {
            if (AgentConnectionHelper.registerAgent(agentState.getMasterURL(), agentState.getLocalAgentInfo())) {
                return true;
            } else {
                return false;
            }
        } else {
            if (AgentConnectionHelper.findRegisteredAgent(agentState.getMasterURL(), agentState.getAgentLocation()) != null) {
                return true;
            } else {
                return false;
            }
        }
    }

}
