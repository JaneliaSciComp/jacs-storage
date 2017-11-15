package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitTester;

public class AgentConnectionTester implements CircuitTester<StorageAgentInfo> {

    @Override
    public boolean testConnection(StorageAgentInfo agentInfo) {
        StorageAgentInfo updatedAgentInfo = AgentConnectionHelper.getAgentStatus(agentInfo.getAgentHttpURL());
        if (updatedAgentInfo != null) {
            return true;
        } else {
            return false;
        }
    }

}
