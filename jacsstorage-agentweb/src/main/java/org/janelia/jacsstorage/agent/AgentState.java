package org.janelia.jacsstorage.agent;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitBreaker;
import org.janelia.jacsstorage.resilience.CircuitBreakerImpl;
import org.janelia.jacsstorage.resilience.CircuitTester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

@ApplicationScoped
public class AgentState {

    private static final Logger LOG = LoggerFactory.getLogger(AgentState.class);

    @PropertyValue(name = "StorageAgent.IPAddress")
    @Inject
    private String storageIPAddress;
    @PropertyValue(name = "StorageAgent.agentLocation")
    @Inject
    private String agentLocation;
    @PropertyValue(name = "StorageAgent.portNo")
    @Inject
    private Integer agentPortNumber;
    @PropertyValue(name = "StorageAgent.storageRootDir")
    @Inject
    private String storageRootDir;
    private String agentURL;
    @Inject @ScheduledResource
    private ScheduledExecutorService scheduler;
    @Inject @PropertyValue(name= "StorageAgent.PingPeriodInSeconds")
    private Integer periodInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.InitialPingDelayInSeconds")
    private Integer initialDelayInSeconds;
    @Inject @PropertyValue(name= "StorageAgent.FailureCountTripThreshold")
    private Integer tripThreshold;

    private CircuitBreaker<AgentState> agentConnectionBreaker;
    private String masterURL;
    private String registeredToken;

    public String getAgentLocation() {
        return StringUtils.isBlank(agentLocation) ? getStorageIPAddress() + "/" + getStorageRootDir() : agentLocation;
    }

    private String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getStorageIPAddress() {
        return StringUtils.isBlank(storageIPAddress) ? getCurrentHostIP() : storageIPAddress;
    }

    public String getConnectionInfo() {
        return getStorageIPAddress() + ":" + agentPortNumber;
    }

    public String getStorageRootDir() {
        return storageRootDir;
    }

    public long getAvailableStorageSpaceInBytes() {
        try {
            java.nio.file.Path storageRootPath = Paths.get(storageRootDir);
            FileStore storageRootStore = Files.getFileStore(storageRootPath);
            long usableBytes = storageRootStore.getUsableSpace();
            return usableBytes;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public synchronized void connectTo(String masterURL) {
        LOG.info("Register agent on {} with {}", this, masterURL);
        Preconditions.checkArgument(StringUtils.isNotBlank(masterURL));
        this.masterURL = masterURL;
        agentConnectionBreaker = new CircuitBreakerImpl<>(
                Optional.empty(), // initial state is undefined
                scheduler,
                periodInSeconds,
                initialDelayInSeconds,
                tripThreshold);

        CircuitTester<AgentState> circuitTester = new AgentConnectionTester();

        agentConnectionBreaker.initialize(this, circuitTester,
                Optional.of(agentState -> {
                    LOG.trace("Agent {} registered with {}", agentState, masterURL);
                }),
                Optional.of(agentState -> {
                    LOG.error("Agent {} got disconnected from {}", agentState, masterURL);
                }));

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                disconnect();
            }
        });
    }

    public synchronized void disconnect() {
        agentConnectionBreaker.dispose();
        AgentConnectionHelper.deregisterAgent(masterURL, agentLocation, registeredToken);
        masterURL = null;
        registeredToken = null;
    }

    public StorageAgentInfo getLocalAgentInfo() {
        StorageAgentInfo localAgentInfo = new StorageAgentInfo(
                getAgentLocation(),
                getAgentURL(),
                getConnectionInfo(),
                getStorageRootDir());
        localAgentInfo.setStorageSpaceAvailableInBytes(getAvailableStorageSpaceInBytes());
        if (this.isRegistered()) {
            localAgentInfo.setConnectionStatus("CONNECTED");
        } else {
            localAgentInfo.setConnectionStatus("DISCONNECTED");
        }
        return localAgentInfo;
    }

    public String getAgentURL() {
        return agentURL;
    }

    public void setAgentURL(String agentURL) {
        this.agentURL = agentURL;
    }

    public String getMasterURL() {
        return masterURL;
    }

    public boolean isRegistered() {
        return StringUtils.isNotBlank(registeredToken);
    }

    public void setRegisteredToken(String registeredToken) {
        this.registeredToken = registeredToken;
    }

    @PostConstruct
    public void initialize() {
        if (StringUtils.isNotBlank(storageRootDir)) {
            try {
                Files.createDirectories(Paths.get(storageRootDir));
            } catch (IOException e) {
                LOG.warn("Error creating storage root directory {}", storageRootDir, e);
            }
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentLocation", agentLocation)
                .append("connectionInfo", getConnectionInfo())
                .append("masterURL", masterURL)
                .append("registeredToken", registeredToken)
                .append("storageIPAddress", storageIPAddress)
                .append("storageRootDir", storageRootDir)
                .build();
    }
}
