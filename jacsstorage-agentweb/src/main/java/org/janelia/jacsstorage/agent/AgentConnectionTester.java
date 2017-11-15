package org.janelia.jacsstorage.agent;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitTester;

import java.util.function.Consumer;

public class AgentConnectionTester implements CircuitTester<AgentState> {

    private final Consumer<AgentState> action;

    public AgentConnectionTester(Consumer<AgentState> action) {
        this.action = action;
    }

    @Override
    public boolean testConnection(AgentState agentState) {
        if (StringUtils.isBlank(agentState.getMasterHttpURL())) {
            return false;
        }
        action.accept(agentState);
        if (!agentState.isRegistered()) {
            StorageAgentInfo registeredAgentInfo = AgentConnectionHelper.registerAgent(agentState.getMasterHttpURL(), agentState.getLocalAgentInfo());
            if (registeredAgentInfo == null) {
                return false;
            } else {
                agentState.setRegisteredToken(registeredAgentInfo.getAgentToken());
                return true;
            }
        } else {
            if (AgentConnectionHelper.findRegisteredAgent(agentState.getMasterHttpURL(), agentState.getAgentHttpURL()) == null) {
                agentState.setRegisteredToken(null);
                return false;
            } else {
                return true;
            }
        }
    }

}
