package org.janelia.jacsstorage.app;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import io.undertow.servlet.api.ListenerInfo;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.service.StorageAgentListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.ServletException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
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
        SeContainer container = containerInit.initialize();
        JacsAgentStorageApp app = container.select(JacsAgentStorageApp.class).get();
        if (StringUtils.isNotBlank(agentArgs.connectTo)) {
            AgentState agentState = container.select(AgentState.class).get();
            LOG.info("Register agent on {} with {}", agentState, agentArgs.connectTo);
            if (!app.registerAgent(agentArgs.connectTo, agentState)) {
                System.err.println("Could not register agent with " + agentArgs.connectTo);
                return;
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    app.deregisterAgent(agentArgs.connectTo, agentState);
                }
            });
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

    private boolean registerAgent(String masterServiceUrl, AgentState agentState) {
        String registrationEndpoint = "/agents";
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);

            StorageAgentInfo agentInfo = new StorageAgentInfo(agentState.getAgentLocation(), agentState.getConnectionInfo(), agentState.getStorageRootDir());
            agentInfo.setStorageSpaceAvailableInMB(agentState.getAvailableStorageSpaceInMB());
            Response response = target.request(MediaType.APPLICATION_JSON_TYPE)
                    .post(Entity.json(agentInfo))
                    ;

            int responseStatus = response.getStatus();
            if (responseStatus == Response.Status.OK.getStatusCode()) {
                return true;
            }
            LOG.warn("Register agent returned {}", responseStatus);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
        return false;
    }

    private void deregisterAgent(String masterServiceUrl, AgentState agentState) {
        String registrationEndpoint = String.format("/agents/%s", agentState.getAgentLocation());
        Client httpClient = null;
        try {
            httpClient = createHttpClient();
            WebTarget target = httpClient.target(masterServiceUrl).path(registrationEndpoint);
            Response response = target.request()
                    .delete()
                    ;
            if (response.getStatus() != Response.Status.NO_CONTENT.getStatusCode()) {
                LOG.warn("Agent deregistration returned {}", response.getStatus());
            }
        } catch (Exception e) {
            LOG.warn("Error raised during agent deregistration", e);
        } finally {
            if (httpClient != null) {
                httpClient.close();
            }
        }
    }

    private Client createHttpClient() throws Exception {
        SSLContext sslContext = SSLContext.getInstance("TLSv1");
        TrustManager[] trustManagers = {
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }
                    @Override
                    public void checkServerTrusted(X509Certificate[] x509Certificates, String authType) throws CertificateException {
                        // Everyone is trusted
                    }
                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }
                }
        };
        sslContext.init(null, trustManagers, new SecureRandom());
        return ClientBuilder.newBuilder()
                .sslContext(sslContext)
                .hostnameVerifier((s, sslSession) -> true)
                .register(new JacksonFeature())
                .build();
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
