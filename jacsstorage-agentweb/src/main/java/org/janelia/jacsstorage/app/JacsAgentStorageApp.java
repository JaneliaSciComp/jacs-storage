package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.undertow.predicate.Predicate;
import io.undertow.predicate.Predicates;
import io.undertow.servlet.api.ListenerInfo;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.agent.AgentState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.core.UriBuilder;
import java.util.concurrent.ExecutorService;

/**
 * This is the agent storage application.
 */
public class JacsAgentStorageApp extends AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(JacsAgentStorageApp.class);
    private static final String DEFAULT_APP_ID = "JacsStorageWorker";

    private static class AgentArgs extends AbstractStorageApp.AppArgs {
        @Parameter(names = "-masterURL", description = "URL of the master datatransfer to which to connect", required = false)
        private String masterHttpUrl;
    }

    public static void main(String[] args) {
        final AgentArgs agentArgs = new AgentArgs();
        JCommander cmdline = new JCommander(agentArgs);
        cmdline.parse(args);
        if (agentArgs.displayUsage) {
            cmdline.usage();
            return;
        }
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit
                .initialize();
        JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();

        AgentState agentState = container.select(AgentState.class).get();
        // update agent info
        agentState.updateAgentInfo(
                UriBuilder.fromPath(app.getRestApi(agentArgs))
                        .scheme("http")
                        .host(agentState.getStorageHost())
                        .port(agentArgs.portNumber)
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
    String getJaxConfigName() {
        return JAXAgentStorageApp.class.getName();
    }

    @Override
    String getRestApi(AppArgs appArgs) {
        StringBuilder apiPathBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(appArgs.baseContextPath)) {
            apiPathBuilder.append(StringUtils.prependIfMissing(appArgs.baseContextPath, "/"));
        }
        apiPathBuilder.append("/agent_api/")
                .append(getApiVersion());
        return apiPathBuilder.toString();
    }

    @Override
    ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

    @Override
    Predicate getAccessLogFilter() {
        return Predicates.not(
                Predicates.prefix("/connection/status")
        );
    }

}
