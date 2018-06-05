package org.janelia.jacsstorage.service.cmd;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.util.client.LoopBackConnectorProvider;
import org.glassfish.jersey.test.util.server.ContainerRequestBuilder;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Application;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@State(Scope.Benchmark)
public class RetrieveBenchmarkResourceTrialParams {
    @Param({""})
    String entriesPathsFile;
    private List<String> entryPathList;
    private ApplicationHandler handler;
    private Client jaxRsClient;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        try {
            setApplicationHandler();
            jaxRsClient = ClientBuilder.newClient(LoopBackConnectorProvider.getClientConfig());
            if (StringUtils.isNotBlank(entriesPathsFile)) {
                entryPathList = Files.readAllLines(Paths.get(entriesPathsFile));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @TearDown
    public void shutdown() {
        if (jaxRsClient != null) {
            jaxRsClient.close();
        }
    }

    private Application setApplicationHandler() {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        JAXAgentStorageApp app = container.select(JAXAgentStorageApp.class).get();
        handler = new ApplicationHandler(app);
        return app;
    }

    public Client getJaxRsClient() {
        return jaxRsClient;
    }

    public ApplicationHandler appHandler() {
        return handler;
    }

    public WebTarget getTarget(URI requestURI) {
        return jaxRsClient.target(requestURI);
    }

    public ContainerRequest request(URI requestURI, String method) {
        return ContainerRequestBuilder.from(requestURI, method).build();

    }

    public String getRandomEntry() {
        return entryPathList.get(RandomUtils.nextInt(0, entryPathList.size()));
    }
}
