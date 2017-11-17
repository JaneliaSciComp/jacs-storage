package org.janelia.jacsstorage.agent;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitBreaker;
import org.janelia.jacsstorage.resilience.CircuitBreakerImpl;
import org.janelia.jacsstorage.resilience.CircuitTester;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.utils.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@ApplicationScoped
public class AgentState {

    private static final Logger LOG = LoggerFactory.getLogger(AgentState.class);

    @Inject
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
    private CircuitBreaker<AgentState> agentConnectionBreaker;
    private String agentHttpURL;
    private int agentTcpPortNo;
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

    public void updateAgentInfo(String agentHttpURL, int agentTcpPortNo) {
        this.agentHttpURL = agentHttpURL;
        this.agentTcpPortNo = agentTcpPortNo;
        getUpdateLocalVolumesAction().accept(this);
    }

    public synchronized void connectTo(String masterHttpURL) {
        LOG.info("Register agent on {} with {}", this, masterHttpURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterHttpURL));
        this.masterHttpURL = masterHttpURL;
        agentConnectionBreaker = new CircuitBreakerImpl<>(
                Optional.empty(), // initial state is undefined
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);

        CircuitTester<AgentState> circuitTester = new AgentConnectionTester(getUpdateLocalVolumesAction());

        agentConnectionBreaker.initialize(this, circuitTester,
                Optional.of(agentState -> {
                    LOG.trace("Agent {} registered with {}", agentState, masterHttpURL);
                }),
                Optional.of(agentState -> {
                    LOG.error("Agent {} got disconnected from {}", agentState, masterHttpURL);
                }));

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
        agentConnectionBreaker.dispose();
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
        StorageAgentInfo localAgentInfo = new StorageAgentInfo(
                localStorageHost,
                agentHttpURL,
                agentTcpPortNo);
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
        agentManagedVolumes = updateStorageVolumes(storageVolumeManager.getManagedVolumes(), (sv) -> true);
    }

    private List<JacsStorageVolume> updateStorageVolumes(List<JacsStorageVolume> storageVolumes, Predicate<JacsStorageVolume> filter) {
        return storageVolumes.stream()
                .filter(filter)
                .map(storageVolume -> {
                    storageVolume.setStorageServiceURL(agentHttpURL);
                    storageVolume.setStorageServiceTCPPortNo(agentTcpPortNo);
                    return storageVolumeManager.updateVolumeInfo(storageVolume);
                })
                .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentTcpPortNo", agentTcpPortNo)
                .append("agentHttpURL", agentHttpURL)
                .append("masterHttpURL", masterHttpURL)
                .append("registeredToken", registeredToken)
                .build();
    }
}
