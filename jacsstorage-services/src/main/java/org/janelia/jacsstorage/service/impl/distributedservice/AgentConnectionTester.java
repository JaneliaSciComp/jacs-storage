package org.janelia.jacsstorage.service.impl.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.resilience.ConnectionTester;

public class AgentConnectionTester implements ConnectionTester<StorageAgentConnection> {

    @Override
    public StorageAgentConnection testConnection(StorageAgentConnection agentConnection) {
        StorageAgentInfo updatedAgentInfo = AgentConnectionHelper.getAgentStatus(agentConnection.getAgentInfo().getAgentAccessURL());
        StorageAgentConnection updatedAgentConnection;
        if (updatedAgentInfo != null) {
            updatedAgentConnection = new StorageAgentConnection(updatedAgentInfo, agentConnection.getConnectionChecker());
            updatedAgentConnection.setConnectStatus(ConnectionState.Status.CLOSED);
        } else {
            updatedAgentConnection = new StorageAgentConnection(agentConnection.getAgentInfo(), agentConnection.getConnectionChecker());
            updatedAgentConnection.setConnectStatus(ConnectionState.Status.OPEN);
        }
        return updatedAgentConnection;
    }

}
