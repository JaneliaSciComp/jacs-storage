package org.janelia.jacsstorage.agent;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionChecker;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.resilience.PeriodicConnectionChecker;
import org.janelia.jacsstorage.resilience.ConnectionTester;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.service.localservice.StorageVolumeBootstrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ApplicationScoped
public class AgentState {

    private static final Logger LOG = LoggerFactory.getLogger(AgentState.class);

    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject @ScheduledResource
    private ScheduledExecutorService scheduler;
    @Inject @PropertyValue(name = "StorageAgent.StorageHost")
    private String storageHost;
    @Inject @PropertyValue(name= "StorageAgent.PingPeriodInSeconds")
    private Integer periodInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.InitialPingDelayInSeconds")
    private Integer initialDelayInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.FailureCountTripThreshold")
    private Integer tripThreshold;
    @Inject
    private NotificationService connectivityNotifier;
    private ConnectionChecker<AgentConnectionState> agentConnectionChecker;
    private String agentHttpURL;
    private List<JacsStorageVolume> agentManagedVolumes;
    private AgentConnectionState connectionState;

    public String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }


    public void updateAgentInfo(String agentHttpURL) {
        LOG.info("Agent URL set to {}", agentHttpURL);
        this.agentHttpURL = agentHttpURL;
        updateStorageVolumes(agentManagedVolumes, (JacsStorageVolume sv) -> !sv.isShared());
    }

    public synchronized void connectTo(String masterHttpURL) {
        LOG.info("Register agent on {} with {}", this, masterHttpURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterHttpURL));
        connectionState = new AgentConnectionState(
                getStorageHost(),
                masterHttpURL,
                agentHttpURL,
                ConnectionState.Status.OPEN,
                0,
                null);
        agentConnectionChecker = new PeriodicConnectionChecker<>(
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);

        ConnectionTester<AgentConnectionState> connectionTester = new AgentConnectionTester();

        agentConnectionChecker.initialize(
                () -> connectionState,
                connectionTester,
                newConnectionState -> {
                    LOG.trace("Agent {} registered with {}", newConnectionState, masterHttpURL);
                    connectionState.setConnectStatus(ConnectionState.Status.CLOSED);
                    connectionState.setConnectionAttempts(0);
                    if (connectionState.isNotRegistered()) {
                        connectionState.setRegisteredToken(newConnectionState.getRegisteredToken());
                    } else if (!newConnectionState.getRegisteredToken().equals(connectionState.getRegisteredToken())) {
                        // the new connection has a new token
                        // this means the connection was re-established
                        connectionState.setRegisteredToken(newConnectionState.getRegisteredToken());
                        connectivityNotifier.sendNotification(
                                "Agent " + agentHttpURL + " reconnected to " + masterHttpURL,
                                "Agent " + agentHttpURL + " reconnected to " + masterHttpURL);
                    }
                    updateStorageVolumes(agentManagedVolumes, (JacsStorageVolume sv) -> !sv.isShared());
                },
                newConnectionState -> {
                    if (newConnectionState.getConnectStatus() != connectionState.getConnectStatus()) {
                        LOG.error("Agent {} got disconnected {}", newConnectionState, masterHttpURL);
                        connectivityNotifier.sendNotification(
                                "Agent " + agentHttpURL + " lost connection to " + masterHttpURL,
                                "Agent " + agentHttpURL + " lost connection to " + masterHttpURL);
                    }
                    connectionState.setConnectStatus(newConnectionState.getConnectStatus());
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
            AgentConnectionHelper.deregisterAgent(
                    connectionState.getMasterHttpURL(),
                    agentHttpURL,
                    connectionState.getRegisteredToken());
            connectionState = null;
        } else {
            LOG.info("Agent {} was not registered or the registration failed", agentHttpURL);
        }
    }

    public StorageAgentInfo getLocalAgentInfo() {
        if (connectionState == null) {
            return new AgentConnectionState(getStorageHost(),
                    null,
                    agentHttpURL,
                    ConnectionState.Status.OPEN,
                    0,
                    null).toStorageAgentInfo();
        } else {
            return connectionState.toStorageAgentInfo();
        }
    }

    @PostConstruct
    public void initialize() {
        agentManagedVolumes = updateStorageVolumes(storageVolumeManager.getManagedVolumes(
                new StorageQuery().setLocalToAnyHost(true)), // only interested in local volumes
                (sv) -> true);
    }

    private List<JacsStorageVolume> updateStorageVolumes(List<JacsStorageVolume> storageVolumes, Predicate<JacsStorageVolume> filter) {
        return storageVolumes.stream()
                .filter(filter)
                .map(storageVolume -> {
                    storageVolume.setStorageServiceURL(agentHttpURL);
                    return storageVolumeManager.updateVolumeInfo(storageVolume);
                })
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentHttpURL", agentHttpURL)
                .append("connectionState", connectionState)
                .build();
    }
}
