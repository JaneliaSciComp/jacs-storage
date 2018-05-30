package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@State(Scope.Benchmark)
public class RetrieveBenchmarkTrialParams extends BenchmarkTrialParams {

    @Param({"0"})
    long nStorageRecords;
    @Param({""})
    String storageEntry;
    private List<String> entryPathList;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        super.setUp(params);
        String nStorageRecordsParam = params.getParam("nStorageRecords");
        if (StringUtils.isNotBlank(nStorageRecordsParam)) {
            nStorageRecords = Long.valueOf(nStorageRecordsParam);
        }
        String entriesPathsFile = params.getParam("entriesPathsFile");
        if (StringUtils.isNotBlank(entriesPathsFile)) {
            try {
                entryPathList = Files.readAllLines(Paths.get(entriesPathsFile));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        if (StringUtils.isNotBlank(dataLocation)) {
            Path tempBenchmarkDataPath = Paths.get(dataLocation);
            if (Files.exists(tempBenchmarkDataPath)) {
                PathUtils.deletePath(tempBenchmarkDataPath);
            }
        }
    }

    public String getRandomEntry() {
        return entryPathList.get(RandomUtils.nextInt(0, entryPathList.size()));
    }
}
