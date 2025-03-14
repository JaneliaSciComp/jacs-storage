package org.janelia.jacsstorage.service.impl.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public interface StorageAgentManager {
    List<StorageAgentInfo> getCurrentRegisteredAgents(Predicate<StorageAgentConnection> agentConnectionPredicate);
    StorageAgentInfo registerAgent(StorageAgentInfo agentInfo);
    StorageAgentInfo deregisterAgent(String agentHttpURL, String agentToken);
    Optional<StorageAgentInfo> findRegisteredAgent(String agentHttpURL);
    Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentConnection> agentConnectionPredicate);
}
