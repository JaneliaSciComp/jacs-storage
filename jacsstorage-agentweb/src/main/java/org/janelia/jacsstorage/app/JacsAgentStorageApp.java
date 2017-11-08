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
        @Parameter(names = "-c", description = "URL of the master service to which to connect", required = false)
        private String connectTo;
    }

    public static void main(String[] args) throws ServletException {
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
        agentState.setAgentURL(UriBuilder.fromPath(agentArgs.baseContextPath)
                .scheme("http")
                .host(agentState.getStorageIPAddress())
                .port(agentArgs.portNumber)
                .path("agent-api")
                .build()
                .toString());
        if (StringUtils.isNotBlank(agentArgs.connectTo)) {
            agentState.connectTo(agentArgs.connectTo);
        }
        StorageAgentListener storageAgentListener = container.select(StorageAgentListener.class).get();
        ExecutorService agentExecutor = container.select(ExecutorService.class).get();
        app.startAgentListener(agentExecutor, storageAgentListener);
        app.start(agentArgs);
    }

    @Override
    protected String getJaxConfigName() {
        return JAXAgentStorageApp.class.getName();
    }

    @Override
    protected String getRestApiMapping() {
        return "/agent-api/*";
    }

    @Override
    protected ListenerInfo[] getAppListeners() {
        return new ListenerInfo[] {
        };
    }

    private void startAgentListener(ExecutorService agentExecutor, StorageAgentListener storageAgentListener) {
        agentExecutor.execute(() -> {
            try {
                storageAgentListener.open();
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
    }
}
