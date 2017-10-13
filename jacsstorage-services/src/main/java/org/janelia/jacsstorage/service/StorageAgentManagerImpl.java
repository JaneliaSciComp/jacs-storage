package org.janelia.jacsstorage.service;

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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ApplicationScoped
public class StorageAgentManagerImpl implements StorageAgentManager {
    private static final Logger LOG = LoggerFactory.getLogger(StorageAgentManagerImpl.class);
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final ConcurrentMap<String, StorageAgentConnection> registeredAgentConnections = new ConcurrentHashMap<>();

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
        if (registeredAgentConnections.putIfAbsent(agentInfo.getLocation(), agentConnection) == null) {
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
            } else if (!storageVolume.equals(agentInfo.getStoragePath())) {
                // warn if path has changed
                LOG.warn("Agent mount point has changed from {} to {}", storageVolume.getMountPoint(), agentInfo.getStoragePath());
                storageVolume.setMountPoint(agentInfo.getStoragePath());
                updatedVolumeFieldsBuilder.put("mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()));
            }
            storageVolumeDao.update(storageVolume, updatedVolumeFieldsBuilder.build());
            agentConnectionBreaker.initialize(agentInfo, new AgentConnectionTester(),
                    Optional.of(storageAgentInfo -> {
                        StorageAgentInfo registeredAgentInfo = registeredAgentConnections.get(storageAgentInfo.getLocation()).getAgentInfo();
                        registeredAgentInfo.setStorageSpaceAvailableInMB(storageAgentInfo.getStorageSpaceAvailableInMB());
                    }),
                    Optional.of(storageAgentInfo -> {
                        LOG.error("Connection lost to {}", storageAgentInfo);
                    }));
        }
        return agentInfo;
    }

    @Override
    public StorageAgentInfo deregisterAgent(String agentLocationInfo) {
        LOG.info("Deregister agent serving {}", agentLocationInfo);
        StorageAgentConnection agentConnection = registeredAgentConnections.remove(agentLocationInfo);
        if (agentConnection == null) {
            return null;
        } else {
            agentConnection.getAgentConnectionBreaker().dispose();
            return agentConnection.getAgentInfo();
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRegisteredAgentByLocationOrConnectionInfo(String agentInfo) {
        StorageAgentConnection agentConnection = registeredAgentConnections.get(agentInfo);
        if (agentConnection != null) {
            agentConnection.updateConnectionStatus();
            return Optional.of(agentConnection.getAgentInfo());
        } else {
            return registeredAgentConnections.entrySet().stream().filter(agentEntry -> agentInfo.equals(agentEntry.getValue()))
                    .findFirst()
                    .map(Map.Entry::getValue)
                    .map((StorageAgentConnection ac) -> {
                        ac.updateConnectionStatus();
                        return ac.getAgentInfo();
                    });
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentInfo> agentFilter) {
        Predicate<StorageAgentConnection> goodConnection = (StorageAgentConnection ac) -> ac.getAgentConnectionBreaker().getState() == CircuitBreaker.BreakerState.CLOSED;
        Predicate<StorageAgentConnection> candidateFilter = goodConnection.and((StorageAgentConnection ac) -> agentFilter.test(ac.getAgentInfo()));
        while (true) {
            long count = registeredAgentConnections.entrySet().stream()
                    .filter(agentEntry -> candidateFilter.test(agentEntry.getValue()))
                    .count();
            if (count == 0) {
                return Optional.empty();
            }
            long pos = RANDOM_SELECTOR.nextInt(registeredAgentConnections.size());
            return registeredAgentConnections.entrySet().stream()
                    .filter(agentEntry -> candidateFilter.test(agentEntry.getValue()))
                    .skip(pos)
                    .findFirst()
                    .map(agentEntry -> {
                        StorageAgentInfo agentInfo = agentEntry.getValue().getAgentInfo();
                        LOG.info("Select {} for storage", agentInfo);
                        return agentInfo;
                    });
        }
    }
}
