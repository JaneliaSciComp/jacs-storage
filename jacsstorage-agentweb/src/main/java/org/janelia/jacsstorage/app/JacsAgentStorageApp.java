package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.service.localservice.StorageVolumeBootstrapper;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;
import java.util.Optional;

/**
 * This is the agent storage application.
 */
public class JacsAgentStorageApp extends AbstractStorageApp {

    private static final String DEFAULT_APP_ID = "JacsStorageWorker";

    private static class AgentArgs extends AppArgs {
        @Parameter(names = "-masterURL", description = "URL of the master datatransfer to which to connect")
        String masterHttpUrl;
        @Parameter(names = "-publicPort", description = "Exposed or public port", required = false)
        Integer publicPortNumber;
        @Parameter(names = "-bootstrapStorageVolumes", description = "Bootstrap agent volumes")
        boolean bootstrapStorageVolumes;
    }

    public static void main(String[] args) {
        final AgentArgs agentArgs = parseAppArgs(args, new AgentArgs());
        if (agentArgs.displayUsage) {
            displayAppUsage(agentArgs);
            return;
        }

        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit
                .initialize();
        JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();

        if (agentArgs.bootstrapStorageVolumes) {
            StorageVolumeBootstrapper volumeBootstrapper = container.select(StorageVolumeBootstrapper.class).get();
            volumeBootstrapper.initializeStorageVolumes();
        }
        AgentState agentState = container.select(AgentState.class).get();
        // update agent info
        int agentPortNumber;
        if (agentArgs.publicPortNumber != null && agentArgs.publicPortNumber > 0) {
            agentPortNumber = agentArgs.publicPortNumber;
        } else {
            agentPortNumber = agentArgs.portNumber;
        }
        agentState.updateAgentInfo(
                UriBuilder.fromPath(new ContextPathBuilder()
                        .path(agentArgs.baseContextPath)
                        .path(app.getRestApiContext())
                        .path(app.getApiVersion())
                        .build())
                        .scheme("http")
                        .host(agentState.getStorageHost())
                        .port(agentPortNumber)
                        .build()
                        .toString());
        if (StringUtils.isNotBlank(agentArgs.masterHttpUrl)) {
            // register agent
            agentState.connectTo(agentArgs.masterHttpUrl);
        }
        // start the HTTP application
        app.start(agentArgs);
    }

    @Override
    String getApplicationId(AppArgs appArgs) {
        if (StringUtils.isBlank(appArgs.applicationId)) {
            return DEFAULT_APP_ID;
        } else {
            return appArgs.applicationId;
        }
    }

    @Override
    Application getJaxApplication() {
        return new JAXAgentStorageApp();
    }

    @Override
    String getRestApiContext() {
        return "agent_api";
    }

    @Override
    String[] getPathsExcludedFromAccessLog() {
        return new String[]{
                "/connection/status"
        };
    }

}
