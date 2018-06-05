package org.janelia.jacsstorage.service.cmd;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.test.JerseyTest;
import org.janelia.jacsstorage.app.JAXAgentStorageApp;
import org.janelia.jacsstorage.service.StorageContentReader;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.ws.rs.client.WebTarget;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@State(Scope.Benchmark)
public class RetrieveBenchmarkResourceTrialParams extends JerseyTest {
    @Param({""})
    String entriesPathsFile;
    private List<String> entryPathList;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        try {
            super.setUp();
            if (StringUtils.isNotBlank(entriesPathsFile)) {
                entryPathList = Files.readAllLines(Paths.get(entriesPathsFile));
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

    }

    protected JAXAgentStorageApp configure() {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        return container.select(JAXAgentStorageApp.class).get();
    }

    public WebTarget getTarget() {
        return super.target();
    }

    public String getRandomEntry() {
        return entryPathList.get(RandomUtils.nextInt(0, entryPathList.size()));
    }
}
