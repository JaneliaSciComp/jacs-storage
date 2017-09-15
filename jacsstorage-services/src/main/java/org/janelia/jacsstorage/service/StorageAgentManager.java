package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import javax.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@ApplicationScoped
public class StorageAgentManager {
    private final Random randomSelector = new Random(System.currentTimeMillis());
    private final ConcurrentMap<String, StorageAgentInfo> registeredAgents = new ConcurrentHashMap<>();

    public List<StorageAgentInfo> getCurrentRegisteredAgents() {
        return ImmutableList.copyOf(registeredAgents.values());
    }

    public StorageAgentInfo registerAgent(StorageAgentInfo agentInfo) {
        registeredAgents.putIfAbsent(agentInfo.getConnectionInfo(), agentInfo);
        return agentInfo;
    }

    public void unregisterAgent(String agentConnectionInfo) {
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
