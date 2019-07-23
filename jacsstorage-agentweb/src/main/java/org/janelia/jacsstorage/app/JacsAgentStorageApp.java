package org.janelia.jacsstorage.app;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.service.localservice.StorageVolumeBootstrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the agent storage application.
 */
public class JacsAgentStorageApp extends AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(JacsAgentStorageApp.class);
    private static final String DEFAULT_APP_ID = "JacsStorageWorker";

    private static class AgentArgs extends AppArgs {
        @Parameter(names = "-masterURL", description = "URL of the master datatransfer to which to connect", required = true)
        String masterHttpUrl;
        @Parameter(names = "-publicPort", description = "Exposed or public port")
        Integer publicPortNumber;
        @Parameter(names = "-bootstrapStorageVolumes", description = "Bootstrap agent volumes")
        boolean bootstrapStorageVolumes;
    }

    public static void main(String[] args) {
        try {
            final AgentArgs agentArgs = parseAppArgs(args, new AgentArgs());
            // validate agentArgs
            if (agentArgs.displayUsage) {
                displayAppUsage(agentArgs);
                return;
            } else if (StringUtils.isBlank(agentArgs.masterHttpUrl)) {
                // this is somehow redundant since the parameter is marked as required
                displayAppUsage(agentArgs, new StringBuilder("'masterURL' parameter is required").append('\n'));
                throw new IllegalStateException("The 'masterURL' parameter is required");
            }

            int agentPortNumber;
            if (agentArgs.publicPortNumber != null && agentArgs.publicPortNumber > 0) {
                agentPortNumber = agentArgs.publicPortNumber;
            } else {
                agentPortNumber = agentArgs.portNumber;
            }

            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();

            ApplicationConfigProvider.setAppDynamicArgs(ImmutableMap.of("StorageAgent.StoragePortNumber", String.valueOf(agentPortNumber)));

            JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();
            ApplicationConfig appConfig = container.select(ApplicationConfig.class, new ApplicationProperties() {
                @Override
                public Class<ApplicationProperties> annotationType() {
                    return ApplicationProperties.class;
                }
            }).get();

            String configuredAgentHost = appConfig.getStringPropertyValue("StorageAgent.StorageHost", NetUtils.getCurrentHostName());
            String storageAgentId = NetUtils.createStorageHostId(configuredAgentHost, String.valueOf(agentPortNumber));

            // update agent info
            AgentState agentState = container.select(AgentState.class).get();
            agentState.initializeAgentState(
                    storageAgentId,
                    UriBuilder.fromPath(new ContextPathBuilder()
                            .path(agentArgs.baseContextPath)
                            .path(app.getRestApiContext())
                            .path(app.getApiVersion())
                            .build())
                            .scheme("http")
                            .host(configuredAgentHost)
                            .port(agentPortNumber)
                            .build()
                            .toString(),
                    "RUNNING");

            // bootstrap storage volumes if needed
            if (agentArgs.bootstrapStorageVolumes) {
                StorageVolumeBootstrapper volumeBootstrapper = container.select(StorageVolumeBootstrapper.class).get();
                volumeBootstrapper.initializeStorageVolumes(configuredAgentHost);
            }
            // update agent state
            agentState.configureAgentServedVolumes();
            // register agent
            agentState.connectTo(agentArgs.masterHttpUrl);
            // start the HTTP application
            app.start(agentArgs, appConfig);
        } catch (Throwable e) {
            LOG.error("Error starting application", e);
        }
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
