package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public interface StorageAgentManager {
    List<StorageAgentInfo> getCurrentRegisteredAgents();
    StorageAgentInfo registerAgent(StorageAgentInfo agentInfo);
    StorageAgentInfo deregisterAgent(String agentConnectionInfo, String agentToken);
    Optional<StorageAgentInfo> findRegisteredAgentByLocationOrConnectionInfo(String agentInfo);
    Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentInfo> agentFilter);
}
