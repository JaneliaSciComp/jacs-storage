package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import java.util.Collection;
import java.util.Random;
import java.util.function.Predicate;

public class OverflowStorageAgentSelector implements StorageAgentSelector {
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final Predicate<StorageAgentConnection> selectorCondition;
    private final String rootDir;

    public OverflowStorageAgentSelector(Predicate<StorageAgentConnection> selectorCondition, String rootDir) {
        this.selectorCondition = selectorCondition;
        this.rootDir = rootDir;
    }

    @Override
    public Predicate<StorageAgentConnection> getSelectorCondition() {
        return selectorCondition;
    }

    @Override
    public StorageAgentInfo selectStorageAgent(Collection<StorageAgentConnection> storageAgentConnections) {
        StorageAgentConnection[] agentConnectionsArray = storageAgentConnections.stream()
                .filter((StorageAgentConnection ac) -> getSelectorCondition().test(ac))
                .toArray(StorageAgentConnection[]::new);
        if (agentConnectionsArray.length == 0) {
            return null;
        }
        int pos = RANDOM_SELECTOR.nextInt(agentConnectionsArray.length);
        StorageAgentConnection selectedAgentConnection = agentConnectionsArray[pos];
        return new StorageAgentInfo(StorageAgentInfo.OVERFLOW_AGENT,
                selectedAgentConnection.getAgentInfo().getAgentURL(),
                selectedAgentConnection.getAgentInfo().getConnectionInfo(),
                rootDir);
    }
}
