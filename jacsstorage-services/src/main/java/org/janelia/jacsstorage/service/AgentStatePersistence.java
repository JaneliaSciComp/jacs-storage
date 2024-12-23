package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;

public interface AgentStatePersistence {

    JacsStorageAgent createAgentStorage(String storageAgentId, String agentAccessURL, String status);

    void updateAgentServedVolumes(JacsStorageAgent jacsStorageAgent);

    void updateAgentStatus(JacsStorageAgent jacsStorageAgent);

    JacsStorageAgent getLocalStorageAgentInfo();
}
