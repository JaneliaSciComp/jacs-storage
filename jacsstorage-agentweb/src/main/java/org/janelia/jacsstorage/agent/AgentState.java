package org.janelia.jacsstorage.agent;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
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

@ApplicationScoped
public class AgentState {

    private static final Logger LOG = LoggerFactory.getLogger(AgentState.class);

    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject
    private AgentStatePersistence agentStatePersistence;
    @Inject @ScheduledResource
    private ScheduledExecutorService scheduler;
    @Inject @PropertyValue(name = "StorageAgent.PingPeriodInSeconds")
    private Integer periodInSeconds;
    @Inject @PropertyValue(name = "StorageAgent.InitialPingDelayInSeconds")
    private Integer initialDelayInSeconds;
    @Inject @PropertyValue(name = "StorageAgent.FailureCountTripThreshold")
    private Integer tripThreshold;
    @Inject @PropertyValue(name = "StorageAgent.ServedVolumes", defaultValue = "*")
    private Set<String> configuredVolumesServed;

    @Inject
    private NotificationService connectivityNotifier;
    private ConnectionChecker<AgentConnectionState> agentConnectionChecker;
    private String agentId;
    private String agentAccessURL;
    private JacsStorageAgent jacsStorageAgent;
    private AgentConnectionState connectionState;

    /**
     * Create and persist storage agent state, i.e. agent host, agent's access URL and agent's status
     * @param agentId
     * @param agentAccessURL
     * @param status
     */
    public void initializeAgentState(String agentId, String agentAccessURL, String status) {
        LOG.info("Agent access set to {} -> {}", agentId, agentAccessURL);
        this.agentId = agentId;
        this.agentAccessURL = agentAccessURL;
        // create persistent agent state if needed
        jacsStorageAgent = agentStatePersistence.createAgentStorage(agentId, agentAccessURL, status);
    }

    public void configureAgentServedVolumes() {
        List<JacsStorageVolume> candidateVolumesForThisAgent = storageVolumeManager.findVolumes(new StorageQuery().setAccessibleOnAgent(agentId).setIncludeInaccessibleVolumes(true));
        Set<String> unreachableVolumeIds = candidateVolumesForThisAgent.stream().filter(sv -> StringUtils.isNotBlank(sv.getStorageServiceURL()))
                .map(sv -> sv.getId().toString())
                .collect(Collectors.toSet());

        if (!configuredVolumesServed.isEmpty() || !unreachableVolumeIds.isEmpty()) {
            LOG.info("Update served volumes for agent running on {} to {}", agentId, configuredVolumesServed);
            JacsStorageAgent updatedStorageAgent = agentStatePersistence.updateAgentServedVolumes(jacsStorageAgent.getId(), configuredVolumesServed, unreachableVolumeIds);
            jacsStorageAgent.setServedVolumes(updatedStorageAgent.getServedVolumes());
        }
        updateStorageOnLocalVolumes(candidateVolumesForThisAgent);
    }

    public synchronized void connectTo(String masterHttpURL) {
        LOG.info("Register agent on {} available at {} with master at {}", agentId, agentAccessURL, masterHttpURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterHttpURL));
        connectionState = new AgentConnectionState(
                agentId,
                masterHttpURL,
                agentAccessURL,
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
                                "Agent " + agentId + " reconnected to " + masterHttpURL,
                                "Agent " + agentId + " available at " + agentAccessURL + " reconnected to " + masterHttpURL);
                    }

                    updateStorageOnLocalVolumes(storageVolumeManager.findVolumes(new StorageQuery().setLocalToAnyAgent(true).setAccessibleOnAgent(agentId)));
                },
                agentConnectionState -> {
                    if (agentConnectionState.getConnectStatus() != connectionState.getConnectStatus()) {
                        LOG.error("Agent {} got disconnected {}", agentConnectionState, masterHttpURL);
                        updateAgentStorageStatus("DISCONNECTED");
                        connectivityNotifier.sendNotification(
                                "Agent " + agentId + " lost connection to " + masterHttpURL,
                                "Agent " + agentId + " available at " + agentAccessURL + " lost connection to " + masterHttpURL);
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

    private synchronized void disconnect() {
        agentConnectionChecker.dispose();
        if (connectionState != null) {
            LOG.info("Unregister agent {} available at {} from master at {}", agentId, agentAccessURL, connectionState.getMasterHttpURL());
            AgentConnectionHelper.deregisterAgent(
                    connectionState.getMasterHttpURL(),
                    agentAccessURL,
                    connectionState.getRegisteredToken());
            updateAgentStorageStatus("DISCONNECTED");
            connectionState = null;
        } else {
            LOG.info("Agent {} with URL {} was not registered or the registration failed", agentId, agentAccessURL);
        }
    }

    public StorageAgentInfo getLocalAgentInfo() {
        if (connectionState == null) {
            return new AgentConnectionState(
                    agentId,
                    null,
                    agentAccessURL,
                    ConnectionState.Status.OPEN,
                    0,
                    null).toStorageAgentInfo(jacsStorageAgent);
        } else {
            return connectionState.toStorageAgentInfo(jacsStorageAgent);
        }
    }

    private void updateAgentStorageStatus(String status) {
        JacsStorageAgent updatedStorageAgent = agentStatePersistence.updateAgentStatus(jacsStorageAgent.getId(), status);
        jacsStorageAgent.setStatus(updatedStorageAgent.getStatus());
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
                .append("agentId", agentId)
                .append("agentHttpURL", agentAccessURL)
                .append("connectionState", connectionState)
                .build();
    }
}
