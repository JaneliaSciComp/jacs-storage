package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionChecker;

public class StorageAgentConnection {

    static final String CONNECTED_STATUS_VALUE = "CONNECTED";
    static final String DISCONNECTED_STATUS_VALUE = "DISCONNECTED";

    private final StorageAgentInfo agentInfo;
    private final ConnectionChecker<StorageAgentInfo> agentConnectionBreaker;

    public StorageAgentConnection(StorageAgentInfo agentInfo, ConnectionChecker<StorageAgentInfo> agentConnectionBreaker) {
        this.agentInfo = agentInfo;
        this.agentConnectionBreaker = agentConnectionBreaker;
    }

    public StorageAgentInfo getAgentInfo() {
        return agentInfo;
    }

    public ConnectionChecker<StorageAgentInfo> getAgentConnectionBreaker() {
        return agentConnectionBreaker;
    }

    public void updateConnectionStatus() {
        if (agentConnectionBreaker.getState() == ConnectionChecker.ConnectionState.CLOSED) {
            agentInfo.setConnectionStatus(CONNECTED_STATUS_VALUE);
        } else {
            agentInfo.setConnectionStatus(DISCONNECTED_STATUS_VALUE);
        }
    }

    public boolean isConnected() {
        return CONNECTED_STATUS_VALUE.equals(agentInfo.getConnectionStatus());
    }
}
