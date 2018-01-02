package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.undertow.servlet.api.ListenerInfo;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.service.StorageAgentListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.servlet.ServletException;
import javax.ws.rs.core.UriBuilder;
import java.util.concurrent.ExecutorService;

/**
 * This is the agent storage application.
 */
public class JacsAgentStorageApp extends AbstractStorageApp {

    private static final Logger LOG = LoggerFactory.getLogger(JacsAgentStorageApp.class);

    private static class AgentArgs extends AbstractStorageApp.AppArgs {
        @Parameter(names = "-masterURL", description = "URL of the master service to which to connect", required = false)
        private String masterHttpUrl;
        @Parameter(names = "-tcpBind", description = "TCP binding IP", required = false)
        private String agentTCPBindingAddress;
        @Parameter(names = "-tcpPort", description = "TCP port number", required = false)
        private int agentTCPPortNumber;
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
        // start the TCP listener
        StorageAgentListener storageAgentListener = container.select(StorageAgentListener.class).get();
        ExecutorService agentExecutor = container.select(ExecutorService.class).get();
        int tcpListenerPortNo = app.startAgentListener(agentArgs, agentExecutor, storageAgentListener);
        // update agent info
        agentState.updateAgentInfo(
                UriBuilder.fromPath(app.getRestApi(agentArgs))
                        .scheme("http")
                        .host(agentState.getStorageHost())
                        .port(agentArgs.portNumber)
                        .build()
                        .toString(),
                tcpListenerPortNo);
        if (StringUtils.isNotBlank(agentArgs.masterHttpUrl)) {
            // register agent
            agentState.connectTo(agentArgs.masterHttpUrl);
        }
        // start the HTTP application
        app.start(agentArgs);
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
        apiPathBuilder.append("/agent-api/")
                .append(getApiVersion());
        return apiPathBuilder.toString();
    }

    @Override
    ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

    private int startAgentListener(AgentArgs args, ExecutorService agentExecutor, StorageAgentListener storageAgentListener) {
        try {
            int listenerPortNumber = storageAgentListener.open(args.agentTCPBindingAddress, args.agentTCPPortNumber);
            agentExecutor.execute(() -> {
                try {
                    storageAgentListener.startServer();
                } catch (Exception e) {
                    LOG.error("Error while running the agent listener", e);
                    throw new IllegalStateException(e);
                } finally {
                    try {
                        storageAgentListener.stopServer();
                    } catch (Exception e) {
                        LOG.error("Error while terminating the agent listener", e);
                    }
                }
            });
            return listenerPortNumber;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
