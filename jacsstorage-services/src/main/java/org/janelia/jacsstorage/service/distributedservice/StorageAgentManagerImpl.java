package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.CircuitBreaker;
import org.janelia.jacsstorage.resilience.CircuitBreakerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.security.SecureRandom;
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
    private final SecureRandom AGENT_TOKEN_GENERATOR = new SecureRandom();

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
    public List<StorageAgentInfo> getCurrentRegisteredAgents(Predicate<StorageAgentConnection> agentConnectionPredicate) {
        return ImmutableList.copyOf(registeredAgentConnections.values().stream()
                .filter(agentConnectionPredicate)
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
        StorageAgentConnection registeredConnection = registeredAgentConnections.putIfAbsent(agentInfo.getAgentHttpURL(), agentConnection);
        if (registeredConnection == null) {
            agentConnection.updateConnectionStatus();
            agentInfo.setAgentToken(String.valueOf(AGENT_TOKEN_GENERATOR.nextInt()));
            agentConnectionBreaker.initialize(agentInfo, new AgentConnectionTester(),
                    Optional.of(storageAgentInfo -> {
                        LOG.trace("Agent {} is up and running", storageAgentInfo.getAgentHttpURL());
                        agentConnection.updateConnectionStatus();
                    }),
                    Optional.of(storageAgentInfo -> {
                        LOG.error("Connection lost to {}", storageAgentInfo);
                        agentConnection.updateConnectionStatus();
                    }));
            return agentInfo;
        } else {
            return registeredConnection.getAgentInfo();
        }
    }

    @Override
    public StorageAgentInfo deregisterAgent(String agentHttpURL, String agentToken) {
        LOG.info("Deregister agent {}", agentHttpURL);
        StorageAgentConnection agentConnection = registeredAgentConnections.get(agentHttpURL);
        if (agentConnection == null) {
            return null;
        } else {
            if (agentConnection.getAgentInfo().getAgentToken().equals(agentToken)) {
                LOG.info("Deregistering agent {} with {}", agentHttpURL, agentToken);
                registeredAgentConnections.remove(agentHttpURL);
                agentConnection.getAgentConnectionBreaker().dispose();
                return agentConnection.getAgentInfo();
            } else {
                throw new IllegalArgumentException("Invalid agent token - deregistration is not allowed with an invalid token");
            }
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRegisteredAgent(String agentHttpURL) {
        StorageAgentConnection agentConnection = registeredAgentConnections.get(agentHttpURL);
        if (agentConnection != null) {
            return Optional.of(agentConnection.getAgentInfo());
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<StorageAgentInfo> findRandomRegisteredAgent(Predicate<StorageAgentConnection> agentConnectionPredicate) {
        StorageAgentConnection[] agentConnectionsArray = registeredAgentConnections.entrySet().stream()
                .filter((Map.Entry<String, StorageAgentConnection> acEntry) -> agentConnectionPredicate.test(acEntry.getValue()))
                .map(acEntry -> acEntry.getValue())
                .toArray(StorageAgentConnection[]::new);
        if (agentConnectionsArray.length == 0) {
            return Optional.empty();
        } else {
            int pos = RANDOM_SELECTOR.nextInt(agentConnectionsArray.length);
            return Optional.of(agentConnectionsArray[pos].getAgentInfo());
        }
    }
}
