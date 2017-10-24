package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import java.util.Collection;
import java.util.Random;
import java.util.function.Predicate;

public class RandomStorageAgentSelector implements StorageAgentSelector {
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final Predicate<StorageAgentConnection> selectorCondition;

    public RandomStorageAgentSelector(Predicate<StorageAgentConnection> selectorCondition) {
        this.selectorCondition = selectorCondition;
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
        return agentConnectionsArray[pos].getAgentInfo();
    }
}
