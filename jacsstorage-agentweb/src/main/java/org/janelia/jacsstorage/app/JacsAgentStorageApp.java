package org.janelia.jacsstorage.app;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.enterprise.inject.spi.AnnotatedType;
import javax.enterprise.inject.spi.Extension;
import javax.enterprise.inject.spi.ProcessAnnotatedType;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.UriBuilder;

import com.beust.jcommander.Parameter;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
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

            /**
             * This extension sets the default values of the PropertyValue annotated fields for:
             *   StorageAgent.StorageHost
             *   StorageAgent.AgentPort
             */
            Extension cmdlineArgsExtension = new Extension() {
                <X> void processAnnotatedType(@Observes ProcessAnnotatedType<X> patEvent) {
                    final AnnotatedType<X> at = patEvent.getAnnotatedType();
                    // ignore types not annotated with PropertyValue
                    if (!at.isAnnotationPresent(PropertyValue.class)) {
                        return;
                    }
                    PropertyValue propertyAnnotation = at.getAnnotation(PropertyValue.class);
                    String name = propertyAnnotation.name();
                    String defaultValue;
                    switch (name) {
                        case "StorageAgent.StorageHost":
                            // set the default for storageHost to the current hostname
                            defaultValue = NetUtils.getCurrentHostName();
                            break;
                        case "StorageAgent.AgentPort":
                            // set the default for agentPort to agentPortNumber argument
                            defaultValue = String.valueOf(agentPortNumber);
                            break;
                        default:
                            // for any other case don't do anything
                            return;
                    }
                    // replace the property value with a new one that has the same name but a different default
                    patEvent.configureAnnotatedType()
                            .remove(a -> a.annotationType().equals(PropertyValue.class))
                            .add(new PropertyValue() {

                                @Override
                                public String name() {
                                    return name;
                                }

                                @Override
                                public String defaultValue() {
                                    return defaultValue;
                                }

                                @Override
                                public Class<PropertyValue> annotationType() {
                                    return PropertyValue.class;
                                }
                            });
                }
            };

            SeContainerInitializer containerInit = SeContainerInitializer.newInstance().addExtensions(cmdlineArgsExtension);
            SeContainer container = containerInit.initialize();
            JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();
            ApplicationConfig appConfig = container.select(ApplicationConfig.class, new ApplicationProperties() {
                @Override
                public Class<ApplicationProperties> annotationType() {
                    return ApplicationProperties.class;
                }
            }).get();

            String configuredAgentHost = appConfig.getStringPropertyValue("StorageAgent.StorageHost", NetUtils.getCurrentHostName());
            String agentHost = configuredAgentHost + ":" + agentPortNumber;

                // bootstrap storage volumes if needed
            if (agentArgs.bootstrapStorageVolumes) {
                StorageVolumeBootstrapper volumeBootstrapper = container.select(StorageVolumeBootstrapper.class).get();
                volumeBootstrapper.initializeStorageVolumes(agentHost);
            }
            AgentState agentState = container.select(AgentState.class).get();
            // update agent info
            agentState.initializeAgentInfo(
                    agentHost,
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
