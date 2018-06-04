package org.janelia.jacsstorage.service.cmd;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.StorageContentReader;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@State(Scope.Benchmark)
public class RetrieveBenchmarkTrialParams {
    @Param({""})
    String entriesPathsFile;
    private List<String> entryPathList;

    StorageContentReader storageContentReader;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        storageContentReader = container.select(StorageContentReader.class).get();

        if (StringUtils.isNotBlank(entriesPathsFile)) {
            try {
                entryPathList = Files.readAllLines(Paths.get(entriesPathsFile));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public String getRandomEntry() {
        return entryPathList.get(RandomUtils.nextInt(0, entryPathList.size()));
    }
}
