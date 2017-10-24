package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitTester;
import org.janelia.jacsstorage.service.distributedservice.AgentConnectionHelper;

public class AgentConnectionTester implements CircuitTester<StorageAgentInfo> {

    @Override
    public boolean testConnection(StorageAgentInfo agentInfo) {
        StorageAgentInfo updatedAgentInfo = AgentConnectionHelper.getAgentStatus(agentInfo.getAgentURL());
        if (updatedAgentInfo != null) {
            return true;
        } else {
            return false;
        }
    }

}
