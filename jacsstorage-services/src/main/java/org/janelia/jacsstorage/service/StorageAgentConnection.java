package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitBreaker;

public class StorageAgentConnection {

    private final StorageAgentInfo agentInfo;
    private final CircuitBreaker<StorageAgentInfo> agentConnectionBreaker;

    public StorageAgentConnection(StorageAgentInfo agentInfo, CircuitBreaker<StorageAgentInfo> agentConnectionBreaker) {
        this.agentInfo = agentInfo;
        this.agentConnectionBreaker = agentConnectionBreaker;
    }

    public StorageAgentInfo getAgentInfo() {
        return agentInfo;
    }

    public CircuitBreaker<StorageAgentInfo> getAgentConnectionBreaker() {
        return agentConnectionBreaker;
    }

    public void updateConnectionStatus() {
        if (agentConnectionBreaker.getState() == CircuitBreaker.BreakerState.CLOSED) {
            agentInfo.setConnectionStatus("CONNECTED");
        } else {
            agentInfo.setConnectionStatus("DISCONNECTED");
        }
    }
}
