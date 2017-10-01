package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;

@ApplicationScoped
public class StorageAgentManagerImpl implements StorageAgentManager {
    private static final Logger LOG = LoggerFactory.getLogger(StorageAgentManagerImpl.class);
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final ConcurrentMap<String, StorageAgentInfo> registeredAgents = new ConcurrentHashMap<>();

    @Override
    public List<StorageAgentInfo> getCurrentRegisteredAgents() {
        return ImmutableList.copyOf(registeredAgents.values());
    }

    @Override
    public StorageAgentInfo registerAgent(StorageAgentInfo agentInfo) {
        LOG.info("Register {}", agentInfo);
        registeredAgents.putIfAbsent(agentInfo.getLocation(), agentInfo);
        return agentInfo;
    }

    @Override
    public void deregisterAgent(String agentLocationInfo) {
        LOG.info("Deregister agent serving {}", agentLocationInfo);
        registeredAgents.remove(agentLocationInfo);
    }

    @Override
    public Optional<StorageAgentInfo> findRegisteredAgentByLocationOrConnectionInfo(String agentInfo) {
        StorageAgentInfo registeredAgent = registeredAgents.get(agentInfo);
        if (registeredAgent != null) {
            return Optional.of(registeredAgent);
        } else {
            return registeredAgents.entrySet().stream().filter(agentEntry -> agentInfo.equals(agentEntry.getValue().getConnectionInfo())).findFirst().map(Map.Entry::getValue);
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentInfo> agentFilter) {
        while (true) {
            long count = registeredAgents.entrySet().stream()
                    .filter(agentEntry -> agentFilter.test(agentEntry.getValue()))
                    .count();
            if (count == 0) {
                return Optional.empty();
            }
            long pos = RANDOM_SELECTOR.nextInt(registeredAgents.size());
            StorageAgentInfo selectedAgentInfo = registeredAgents.entrySet().stream()
                    .filter(agentEntry -> agentFilter.test(agentEntry.getValue()))
                    .skip(pos)
                    .findFirst()
                    .map(Map.Entry::getValue).orElse(null);
            if (selectedAgentInfo != null) {
                LOG.info("Select {} for storage", selectedAgentInfo);
                return Optional.of(selectedAgentInfo);
            }
        }
    }
}
