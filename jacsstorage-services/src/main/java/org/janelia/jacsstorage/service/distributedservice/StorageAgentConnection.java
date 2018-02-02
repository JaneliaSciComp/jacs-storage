package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionChecker;

public class StorageAgentConnection {

    static final String CONNECTED_STATUS_VALUE = "CONNECTED";
    static final String DISCONNECTED_STATUS_VALUE = "DISCONNECTED";

    private final StorageAgentInfo agentInfo;
    private final ConnectionChecker<StorageAgentInfo> agentConnectionChecker;

    public StorageAgentConnection(StorageAgentInfo agentInfo, ConnectionChecker<StorageAgentInfo> agentConnectionChecker) {
        this.agentInfo = agentInfo;
        this.agentConnectionChecker = agentConnectionChecker;
    }

    public StorageAgentInfo getAgentInfo() {
        return agentInfo;
    }

    public ConnectionChecker<StorageAgentInfo> getAgentConnectionChecker() {
        return agentConnectionChecker;
    }

    public void updateConnectionStatus() {
        if (agentConnectionChecker.getState() == ConnectionChecker.ConnectionState.CLOSED) {
            agentInfo.setConnectionStatus(CONNECTED_STATUS_VALUE);
        } else {
            agentInfo.setConnectionStatus(DISCONNECTED_STATUS_VALUE);
        }
    }

    public boolean isConnected() {
        return CONNECTED_STATUS_VALUE.equals(agentInfo.getConnectionStatus());
    }
}
