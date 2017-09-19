package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.slf4j.Logger;

import javax.enterprise.inject.Vetoed;
import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * This class is explicitly excluded from CDI because it's created using the method from the @ServicesProducer.
 */
@Vetoed
public class StorageAgentManagerImpl implements StorageAgentManager {
    private final Random randomSelector = new Random(System.currentTimeMillis());
    private final ConcurrentMap<String, StorageAgentInfo> registeredAgents = new ConcurrentHashMap<>();
    private final Logger logger;

    @Inject
    public StorageAgentManagerImpl(Logger logger) {
        this.logger = logger;
    }

    public List<StorageAgentInfo> getCurrentRegisteredAgents() {
        return ImmutableList.copyOf(registeredAgents.values());
    }

    public StorageAgentInfo registerAgent(StorageAgentInfo agentInfo) {
        logger.info("Register {}", agentInfo);
        registeredAgents.putIfAbsent(agentInfo.getConnectionInfo(), agentInfo);
        return agentInfo;
    }

    public void deregisterAgent(String agentConnectionInfo) {
        logger.info("Deregister agent running at {}", agentConnectionInfo);
        registeredAgents.remove(agentConnectionInfo);
    }

    public Optional<StorageAgentInfo> findRandomRegisteredAgent() {
        while (true) {
            if (registeredAgents.size() == 0) {
                return Optional.empty();
            }
            int pos = randomSelector.nextInt(registeredAgents.size());
            StorageAgentInfo selectedAgentInfo = registeredAgents.entrySet().stream().skip(pos).findFirst().map(ae -> ae.getValue()).orElse(null);
            if (selectedAgentInfo != null) {
                return Optional.of(selectedAgentInfo);
            }
        }
    }
}
