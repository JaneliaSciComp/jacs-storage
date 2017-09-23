package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public interface StorageAgentManager {
    List<StorageAgentInfo> getCurrentRegisteredAgents();
    StorageAgentInfo registerAgent(StorageAgentInfo agentInfo);
    void deregisterAgent(String agentConnectionInfo);
    Optional<StorageAgentInfo> findRegisteredAgentByLocationOrConnectionInfo(String agentInfo);
    Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentInfo> agentFilter);
}
