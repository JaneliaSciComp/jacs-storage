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
import org.janelia.jacsstorage.resilience.PeriodicConnectionChecker;
import org.janelia.jacsstorage.resilience.ConnectionTester;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.coreutils.NetUtils;
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
    private ConnectionChecker<AgentState> agentConnectionChecker;
    private String agentHttpURL;
    private List<JacsStorageVolume> agentManagedVolumes;
    private String masterHttpURL;
    private String registeredToken;

    public String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

    public String getMasterHttpURL() {
        return masterHttpURL;
    }

    public String getAgentHttpURL() {
        return agentHttpURL;
    }

    public void updateAgentInfo(String agentHttpURL) {
        this.agentHttpURL = agentHttpURL;
        getUpdateLocalVolumesAction().accept(this);
    }

    public synchronized void connectTo(String masterHttpURL) {
        LOG.info("Register agent on {} with {}", this, masterHttpURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterHttpURL));
        this.masterHttpURL = masterHttpURL;
        agentConnectionChecker = new PeriodicConnectionChecker<>(
                null, // initial state is undefined
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);

        ConnectionTester<AgentState> connectionTester = new AgentConnectionTester(getUpdateLocalVolumesAction());

        agentConnectionChecker.initialize(this, connectionTester,
                agentState -> {
                    LOG.trace("Agent {} registered with {}", agentState, masterHttpURL);
                },
                agentState -> {
                    LOG.error("Agent {} got disconnected from {}", agentState, masterHttpURL);
                    connectivityNotifier.sendNotification(
                            "Agent " + agentHttpURL + " lost connection to " + masterHttpURL,
                            "Agent " + agentHttpURL + " lost connection to " + masterHttpURL);
                });

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                disconnect();
            }
        });
    }

    private Consumer<AgentState> getUpdateLocalVolumesAction() {
        return (AgentState as) -> {
            as.updateStorageVolumes(as.agentManagedVolumes, (JacsStorageVolume sv) -> !sv.isShared());
        };
    }

    public synchronized void disconnect() {
        agentConnectionChecker.dispose();
        if (registeredToken != null) {
            AgentConnectionHelper.deregisterAgent(masterHttpURL, agentHttpURL, registeredToken);
        } else {
            LOG.info("Agent {} was not registered or the registration failed", agentHttpURL);
        }
        masterHttpURL = null;
        registeredToken = null;
    }

    public StorageAgentInfo getLocalAgentInfo() {
        String localStorageHost = getStorageHost();
        StorageAgentInfo localAgentInfo = new StorageAgentInfo(localStorageHost, agentHttpURL);
        if (this.isRegistered()) {
            localAgentInfo.setConnectionStatus("CONNECTED");
        } else {
            localAgentInfo.setConnectionStatus("DISCONNECTED");
        }
        return localAgentInfo;
    }

    public boolean isRegistered() {
        return StringUtils.isNotBlank(registeredToken);
    }

    public void setRegisteredToken(String registeredToken) {
        this.registeredToken = registeredToken;
    }

    @PostConstruct
    public void initialize() {
        agentManagedVolumes = updateStorageVolumes(storageVolumeManager.getManagedVolumes(new StorageQuery()), (sv) -> true);
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
                .append("masterHttpURL", masterHttpURL)
                .append("registeredToken", registeredToken)
                .build();
    }
}
