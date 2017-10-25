package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.resilience.CircuitBreaker;
import org.janelia.jacsstorage.resilience.CircuitBreakerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ApplicationScoped
public class StorageAgentManagerImpl implements StorageAgentManager {
    private static final Logger LOG = LoggerFactory.getLogger(StorageAgentManagerImpl.class);

    private final ConcurrentMap<String, StorageAgentConnection> registeredAgentConnections = new ConcurrentHashMap<>();
    private final SecureRandom AGENT_TOKEN_GENERATOR = new SecureRandom();

    @Inject
    private JacsStorageVolumeDao storageVolumeDao;
    @Inject @ScheduledResource
    private ScheduledExecutorService scheduler;
    @Inject @PropertyValue(name= "StorageAgent.PingPeriodInSeconds")
    private Integer periodInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.InitialPingDelayInSeconds")
    private Integer initialDelayInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.FailureCountTripThreshold")
    private Integer tripThreshold;
    @Inject @PropertyValue(name = "Storage.Overflow.RootDir")
    private String overflowRootDir;

    @Override
    public List<StorageAgentInfo> getCurrentRegisteredAgents() {
        return ImmutableList.copyOf(registeredAgentConnections.values().stream()
                .map(agentConnection -> {
                    agentConnection.updateConnectionStatus();
                    return agentConnection.getAgentInfo();
                })
                .collect(Collectors.toList()));
    }

    @Override
    public StorageAgentInfo registerAgent(StorageAgentInfo agentInfo) {
        LOG.info("Register {}", agentInfo);
        CircuitBreaker<StorageAgentInfo> agentConnectionBreaker = new CircuitBreakerImpl<>(
                Optional.of(CircuitBreaker.BreakerState.CLOSED),
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);
        StorageAgentConnection agentConnection = new StorageAgentConnection(agentInfo, agentConnectionBreaker);
        StorageAgentConnection registeredConnection = registeredAgentConnections.putIfAbsent(agentInfo.getLocation(), agentConnection);
        if (registeredConnection == null) {
            agentInfo.setAgentToken(String.valueOf(AGENT_TOKEN_GENERATOR.nextInt()));
            // update connection info for the volume in case anything has changed.
            JacsStorageVolume storageVolume = storageVolumeDao.getStorageByLocationAndCreateIfNotFound(agentInfo.getLocation());
            ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
            storageVolume.setMountHostIP(agentInfo.getConnectionInfo());
            updatedVolumeFieldsBuilder.put("mountHostIP", new SetFieldValueHandler<>(storageVolume.getMountHostIP()));
            storageVolume.setMountHostURL(agentInfo.getAgentURL());
            updatedVolumeFieldsBuilder.put("mountHostURL", new SetFieldValueHandler<>(storageVolume.getMountHostURL()));
            if (StringUtils.isBlank(storageVolume.getMountPoint())) {
                storageVolume.setMountPoint(agentInfo.getStoragePath());
                updatedVolumeFieldsBuilder.put("mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()));
            } else if (!storageVolume.getMountPoint().equals(agentInfo.getStoragePath())) {
                // warn if path has changed
                LOG.warn("Agent mount point has changed from {} to {}", storageVolume.getMountPoint(), agentInfo.getStoragePath());
                storageVolume.setMountPoint(agentInfo.getStoragePath());
                updatedVolumeFieldsBuilder.put("mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()));
            }
            storageVolumeDao.update(storageVolume, updatedVolumeFieldsBuilder.build());
            agentConnectionBreaker.initialize(agentInfo, new AgentConnectionTester(),
                    Optional.of(storageAgentInfo -> {
                        StorageAgentInfo registeredAgentInfo = registeredAgentConnections.get(storageAgentInfo.getLocation()).getAgentInfo();
                        registeredAgentInfo.setStorageSpaceAvailableInBytes(storageAgentInfo.getStorageSpaceAvailableInBytes());
                    }),
                    Optional.of(storageAgentInfo -> {
                        LOG.error("Connection lost to {}", storageAgentInfo);
                    }));
            return agentInfo;
        } else {
            return registeredConnection.getAgentInfo();
        }
    }

    @Override
    public StorageAgentInfo deregisterAgent(String agentLocationInfo, String agentToken) {
        LOG.info("Deregister agent serving {}", agentLocationInfo);
        StorageAgentConnection agentConnection = registeredAgentConnections.get(agentLocationInfo);
        if (agentConnection == null) {
            return null;
        } else {
            if (agentConnection.getAgentInfo().getAgentToken().equals(agentToken)) {
                LOG.info("Deregistering agent for {} with {}", agentLocationInfo, agentToken);
                registeredAgentConnections.remove(agentLocationInfo);
                agentConnection.getAgentConnectionBreaker().dispose();
                return agentConnection.getAgentInfo();
            } else {
                throw new IllegalArgumentException("Invalid agent token - deregistration is not allowed with an invalid token");
            }
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRegisteredAgentByLocationOrConnectionInfo(String agentInfo) {
        if (StorageAgentInfo.OVERFLOW_AGENT.equals(agentInfo)) {
            Predicate<StorageAgentConnection> goodConnection = (StorageAgentConnection ac) -> ac.getAgentConnectionBreaker().getState() == CircuitBreaker.BreakerState.CLOSED;
            StorageAgentInfo storageAgentInfo = new OverflowStorageAgentSelector(goodConnection, overflowRootDir)
                    .selectStorageAgent(registeredAgentConnections.values());
            if (storageAgentInfo != null) {
                return Optional.of(storageAgentInfo);
            } else {
                // no agent is registered or there's no good connection to any of the registered agents.
                return Optional.empty();
            }
        } else {
            // search by location info
            StorageAgentConnection agentConnection = registeredAgentConnections.get(agentInfo);
            if (agentConnection != null) {
                agentConnection.updateConnectionStatus();
                return Optional.of(agentConnection.getAgentInfo());
            } else {
                // search by connection info
                return registeredAgentConnections.entrySet().stream()
                        .filter(agentEntry -> agentInfo.equals(agentEntry.getValue().getAgentInfo().getConnectionInfo()))
                        .findFirst()
                        .map(Map.Entry::getValue)
                        .map((StorageAgentConnection ac) -> {
                            ac.updateConnectionStatus();
                            return ac.getAgentInfo();
                        });
            }
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentInfo> agentFilter) {
        Predicate<StorageAgentConnection> goodConnection = (StorageAgentConnection ac) -> ac.getAgentConnectionBreaker().getState() == CircuitBreaker.BreakerState.CLOSED;
        Predicate<StorageAgentConnection> agentSelectableCondition = (StorageAgentConnection ac) -> ac.getAgentInfo().getStorageSpaceAvailableInBytes() > 0;
        Predicate<StorageAgentConnection> candidateFilter = (StorageAgentConnection ac) -> agentFilter == null || agentFilter.test(ac.getAgentInfo());
        StorageAgentSelector[] agentSelectors = new StorageAgentSelector[] {
                new RandomStorageAgentSelector(goodConnection.and(agentSelectableCondition).and(candidateFilter)),
                new OverflowStorageAgentSelector(goodConnection, overflowRootDir)
        };
        Collection<StorageAgentConnection> agentConnections = registeredAgentConnections.values();
        for (StorageAgentSelector agentSelector : agentSelectors) {
            StorageAgentInfo agentInfo = agentSelector.selectStorageAgent(agentConnections);
            if (agentInfo != null) return Optional.of(agentInfo);
        }
        return Optional.empty();
    }
}
