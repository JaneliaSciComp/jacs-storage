package org.janelia.jacsstorage.agent.cmd;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.client.ClientConfig;
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
public class RetrieveBenchmarkResourceTrialParams extends JerseyTest {
    @Param({""})
    String entriesPathsFile;
    private List<String> entryPathList;
    private Application application;
    private ApplicationHandler handler;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        try {
            setApplicationHandler();
            super.setUp();
            if (StringUtils.isNotBlank(entriesPathsFile)) {
                entryPathList = Files.readAllLines(Paths.get(entriesPathsFile));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Application configure() {
        setApplication();
        return application;
    }

    @TearDown
    public void shutdown() {
        try {
            super.tearDown();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private void setApplicationHandler() {
        setApplication();
        handler = new ApplicationHandler(application);
    }

    private void setApplication() {
        if (application == null) {
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();
            application = container.select(JAXAgentStorageApp.class).get();
        }
    }

    public ApplicationHandler appHandler() {
        return handler;
    }

    public ContainerRequest request(URI requestURI, String method) {
        return ContainerRequestBuilder.from(requestURI, method).build();

    }

    public String getRandomEntry() {
        return entryPathList.get(RandomUtils.nextInt(0, entryPathList.size()));
    }
}
