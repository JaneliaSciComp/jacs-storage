package org.janelia.jacsstorage.service.cmd;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.janelia.jacsstorage.service.DataContentService;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class RetrieveBenchmarkTrialParams {
    @Param({""})
    String s3EntriesFile;
    private List<String> s3Entries = new ArrayList<>();

    @Param({""})
    String fsEntriesFile;
    private List<String> fsEntries = new ArrayList<>();

    private final UniformRandomProvider rng = RandomSource.create(RandomSource.XO_RO_SHI_RO_128_PP);

    DataContentService storageContentReader;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        storageContentReader = container.select(DataContentService.class).get();

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
    }

    public String getRandomS3Entry() {
        return s3Entries.isEmpty() ? null : s3Entries.get(rng.nextInt(s3Entries.size()));
    }

    public String getRandomFSEntry() {
        return fsEntries.isEmpty() ? null : fsEntries.get(rng.nextInt(fsEntries.size()));
    }
}
