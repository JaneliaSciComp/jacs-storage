package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import java.util.Collection;
import java.util.function.Predicate;

public interface StorageAgentSelector {
    Predicate<StorageAgentConnection> getSelectorCondition();
    StorageAgentInfo selectStorageAgent(Collection<StorageAgentConnection> storageAgentConnections);
}
