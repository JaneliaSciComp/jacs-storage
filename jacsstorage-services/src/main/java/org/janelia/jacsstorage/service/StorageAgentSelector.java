package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public interface StorageAgentSelector {
    String OVERFLOW_AGENT_INFO = "OVERFLOW_AGENT";

    Predicate<StorageAgentConnection> getSelectorCondition();
    StorageAgentInfo selectStorageAgent(Collection<StorageAgentConnection> storageAgentConnections);
}
