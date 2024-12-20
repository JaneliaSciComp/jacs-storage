package org.janelia.jacsstorage.agent.cmd;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;
import jakarta.ws.rs.core.Application;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.test.JerseyTest;
import org.glassfish.jersey.test.util.server.ContainerRequestBuilder;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class RetrieveBenchmarkResourceTrialParams extends JerseyTest {
    @Param({""})
    String s3EntriesFile;
    private List<String> s3Entries = new ArrayList<>();

    @Param({""})
    String fsEntriesFile;
    private List<String> fsEntries = new ArrayList<>();

    @Param({""})
    String storageVolumeId;

    private Application application;
    private ApplicationHandler handler;
    private final UniformRandomProvider rng = RandomSource.create(RandomSource.XO_RO_SHI_RO_128_PP);

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        try {
            setApplicationHandler();
            super.setUp();
            if (StringUtils.isNotBlank(s3EntriesFile)) {
                try {
                    s3Entries = Files.readAllLines(Paths.get(s3EntriesFile));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            if (StringUtils.isNotBlank(fsEntriesFile)) {
                try {
                    fsEntries = Files.readAllLines(Paths.get(fsEntriesFile));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
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
        return ContainerRequestBuilder
                .from(requestURI, method, null).build();

    }

    public String getRandomS3Entry() {
        return s3Entries.isEmpty() ? null : s3Entries.get(rng.nextInt(s3Entries.size()));
    }

    public String getRandomFSEntry() {
        return fsEntries.isEmpty() ? null : fsEntries.get(rng.nextInt(fsEntries.size()));
    }
}
