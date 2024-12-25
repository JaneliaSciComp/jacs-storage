package org.janelia.jacsstorage.agent;

import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Produces;

import org.janelia.jacsstorage.agent.impl.AgentStateImpl;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.cdi.qualifier.ScheduledResource;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Annotating the agent state implementation with ApplicationScoped does not seem to create
 * a single AgentState instance per application so the AgentStateProvider will take care of that
 * by holding a static reference to an AgentState instance.
 */
@ApplicationScoped
public class AgentStateProvider {

    private static final Logger LOG = LoggerFactory.getLogger(AgentStateProvider.class);

    private static AgentState agentStateInstance;

    @ApplicationScoped
    @Produces
    public AgentState agentState(
            @LocalInstance StorageVolumeManager storageVolumeManager,
            AgentStatePersistence agentStatePersistence,
            @ScheduledResource ScheduledExecutorService scheduler,
            NotificationService connectivityNotifier,
            @PropertyValue(name = "StorageAgent.ServedVolumes", defaultValue = "*") Set<String> configuredServedVolumes,
            @PropertyValue(name = "StorageAgent.PingPeriodInSeconds") Integer periodInSeconds,
            @PropertyValue(name = "StorageAgent.InitialPingDelayInSeconds") Integer initialDelayInSeconds,
            @PropertyValue(name = "StorageAgent.FailureCountTripThreshold") Integer tripThreshold) {
        if (agentStateInstance == null) {
            agentStateInstance = new AgentStateImpl(
                    storageVolumeManager,
                    agentStatePersistence,
                    scheduler,
                    connectivityNotifier,
                    configuredServedVolumes,
                    periodInSeconds,
                    initialDelayInSeconds,
                    tripThreshold);
        }
        return agentStateInstance;
    }

    public void disconnect(@Disposes AgentState agentState) {
        if (agentState != null) {
            LOG.info("Disconnect {}", agentState);
            agentState.disconnect();
        }
    }

}
