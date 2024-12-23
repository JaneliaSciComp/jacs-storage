package org.janelia.jacsstorage.agent;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;

public interface AgentState {
    String getLocalAgentId();
    StorageAgentInfo getLocalAgentInfo();
    void initializeAgentState(String storageAgentId, String storageAgentURL, String status);
    boolean isInitialized();
    void connectTo(String masterHttpURL);
    void disconnect();
}
