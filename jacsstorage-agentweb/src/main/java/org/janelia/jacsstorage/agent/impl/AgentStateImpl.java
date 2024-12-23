package org.janelia.jacsstorage.agent.impl;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.resilience.ConnectionChecker;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.resilience.ConnectionTester;
import org.janelia.jacsstorage.resilience.PeriodicConnectionChecker;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AgentStateImpl implements AgentState {

    private static final Logger LOG = LoggerFactory.getLogger(AgentStateImpl.class);

    private final StorageVolumeManager storageVolumeManager;
    private final AgentStatePersistence agentStatePersistence;
    private final ScheduledExecutorService scheduler;
    private final NotificationService connectivityNotifier;
    private final Set<String> configuredServedVolumes;
    private final Integer periodInSeconds;
    private final Integer initialDelayInSeconds;
    private final Integer tripThreshold;

    private String storageAgentId;
    private String storageAgentURL;

    private ConnectionChecker<AgentConnectionState> agentConnectionChecker;
    private JacsStorageAgent jacsStorageAgent;
    private AgentConnectionState connectionState;

    public AgentStateImpl(StorageVolumeManager storageVolumeManager,
                          AgentStatePersistence agentStatePersistence,
                          ScheduledExecutorService scheduler,
                          NotificationService connectivityNotifier,
                          Set<String> configuredServedVolumes,
                          Integer periodInSeconds,
                          Integer initialDelayInSeconds,
                          Integer tripThreshold) {
        this.storageVolumeManager = storageVolumeManager;
        this.agentStatePersistence = agentStatePersistence;
        this.scheduler = scheduler;
        this.connectivityNotifier = connectivityNotifier;
        this.configuredServedVolumes = configuredServedVolumes;
        this.periodInSeconds = periodInSeconds;
        this.initialDelayInSeconds = initialDelayInSeconds;
        this.tripThreshold = tripThreshold;
    }

    /**
     * Create and persist storage agent state, i.e. agent host, agent's access URL and agent's status
     *
     * @param storageAgentId
     * @param storageAgentURL
     * @param status
     */
    @Override
    public void initializeAgentState(String storageAgentId, String storageAgentURL, String status) {
        LOG.info("!!!!!!!!!!!!!!!Agent status set for {}:{} to {}", storageAgentId, storageAgentURL, status);
        this.storageAgentId = storageAgentId;
        this.storageAgentURL = storageAgentURL;
        // create persistent agent state if needed
        jacsStorageAgent = agentStatePersistence.createAgentStorage(storageAgentId, storageAgentURL, status);
        setAvailableStorageVolumes();
    }

    public boolean isInitialized() {
        return StringUtils.isNotBlank(storageAgentId);
    }

    private void setAvailableStorageVolumes() {
        List<JacsStorageVolume> candidateVolumesForThisAgent = storageVolumeManager.findVolumes(
                new StorageQuery()
                        .setAccessibleOnAgent(storageAgentId)
                        .setIncludeInaccessibleVolumes(true));
        Set<String> unreachableVolumeIds = candidateVolumesForThisAgent.stream()
                .filter(sv -> StringUtils.isBlank(sv.getStorageServiceURL())) // the unreachable volumes do not have a storageServiceURL set
                .map(sv -> sv.getId().toString())
                .collect(Collectors.toSet());

        jacsStorageAgent.setServedVolumes(configuredServedVolumes);
        jacsStorageAgent.setUnavailableVolumeIds(unreachableVolumeIds);
        LOG.info("Update served volumes for agent {}", jacsStorageAgent);
        agentStatePersistence.updateAgentServedVolumes(jacsStorageAgent);

        updateStorageOnLocalVolumes(candidateVolumesForThisAgent);
    }

    @Override
    public synchronized void connectTo(String masterHttpURL) {
        LOG.info("Register agent on {}:{} available at {} with master at {}", storageAgentId, jacsStorageAgent, storageAgentURL, masterHttpURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterHttpURL));
        connectionState = new AgentConnectionState(
                storageAgentId,
                masterHttpURL,
                storageAgentURL,
                ConnectionState.Status.OPEN,
                0,
                null);
        agentConnectionChecker = new PeriodicConnectionChecker<>(
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);
        ConnectionTester<AgentConnectionState> connectionTester = new AgentConnectionTester(jacsStorageAgent);

        agentConnectionChecker.initialize(
                () -> connectionState,
                connectionTester,
                agentConnectionState -> {
                    LOG.trace("Agent {} registered with {}", agentConnectionState, masterHttpURL);
                    connectionState.setConnectStatus(ConnectionState.Status.CLOSED);
                    updateAgentStorageStatus("CONNECTED");
                    connectionState.setConnectionAttempts(0);
                    if (connectionState.isNotRegistered()) {
                        connectionState.setRegisteredToken(agentConnectionState.getRegisteredToken());
                    } else if (!agentConnectionState.getRegisteredToken().equals(connectionState.getRegisteredToken())) {
                        // the new connection has a new token
                        // this means the connection was re-established
                        connectionState.setRegisteredToken(agentConnectionState.getRegisteredToken());
                        connectivityNotifier.sendNotification(
                                "Agent " + storageAgentId + " reconnected to " + masterHttpURL,
                                "Agent " + storageAgentId + " available at " + storageAgentURL + " reconnected to " + masterHttpURL);
                    }

                    setAvailableStorageVolumes(); // this should probably not be done as often as the connection check
                },
                agentConnectionState -> {
                    if (agentConnectionState.getConnectStatus() != connectionState.getConnectStatus()) {
                        LOG.error("Agent {} got disconnected {}", agentConnectionState, masterHttpURL);
                        updateAgentStorageStatus("DISCONNECTED");
                        connectivityNotifier.sendNotification(
                                "Agent " + storageAgentId + " lost connection to " + masterHttpURL,
                                "Agent " + storageAgentId + " available at " + storageAgentURL + " lost connection to " + masterHttpURL);
                    }
                    connectionState.setConnectStatus(agentConnectionState.getConnectStatus());
                });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                disconnect();
            }
        });
    }

    public synchronized void disconnect() {
        agentConnectionChecker.dispose();
        if (connectionState != null) {
            LOG.info("Unregister agent {} available at {} from master at {}",
                    storageAgentId, storageAgentURL, connectionState.getMasterHttpURL());
            AgentConnectionHelper.deregisterAgent(
                    connectionState.getMasterHttpURL(),
                    storageAgentURL,
                    connectionState.getRegisteredToken());
            updateAgentStorageStatus("DISCONNECTED");
            connectionState = null;
        } else {
            LOG.info("Agent {} with URL {} was not registered or the registration failed",
                    storageAgentId, storageAgentURL);
        }
    }

    @Override
    public String getLocalAgentId() {
        return storageAgentId;
    }

    @Override
    public StorageAgentInfo getLocalAgentInfo() {
        if (connectionState == null) {
            if (jacsStorageAgent == null) {
                // initialize agent info
                jacsStorageAgent = agentStatePersistence.createAgentStorage(storageAgentId, storageAgentURL, "RUNNING");
                setAvailableStorageVolumes();
            }
            return new AgentConnectionState(
                    storageAgentId,
                    null,
                    storageAgentURL,
                    ConnectionState.Status.OPEN,
                    0,
                    null).toStorageAgentInfo(jacsStorageAgent);
        } else {
            return connectionState.toStorageAgentInfo(jacsStorageAgent);
        }
    }

    private void updateAgentStorageStatus(String status) {
        if (jacsStorageAgent != null) {
            jacsStorageAgent.setStatus(status);
            agentStatePersistence.updateAgentStatus(jacsStorageAgent);
        }
    }

    private void updateStorageOnLocalVolumes(List<JacsStorageVolume> volumeList) {
        volumeList.stream()
                .filter(v -> !v.isShared())
                .filter(v -> StringUtils.isNotBlank(v.getStorageServiceURL()))
                .forEach(sv -> {
                    storageVolumeManager.updateVolumeInfo(sv.getId(), sv);
                });
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentId", storageAgentId)
                .append("agentHttpURL", storageAgentURL)
                .append("connectionState", connectionState)
                .build();
    }
}
