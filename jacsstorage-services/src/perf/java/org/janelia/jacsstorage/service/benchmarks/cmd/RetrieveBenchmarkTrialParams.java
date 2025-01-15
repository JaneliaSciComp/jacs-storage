package org.janelia.jacsstorage.service.benchmarks.cmd;

import jakarta.enterprise.inject.se.SeContainer;
import jakarta.enterprise.inject.se.SeContainerInitializer;

import org.janelia.jacsstorage.service.DataContentService;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class RetrieveBenchmarkTrialParams extends AbstractBenchmarkTrialParams {
    @Param("false")
    boolean useAsync;

    @Param("false")
    boolean applyBloscDecompression;

    private SeContainer container;
    DataContentService storageContentReader;

    @Setup(Level.Trial)
    public void setUpTrial(BenchmarkParams params) {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        container = containerInit.initialize();
        storageContentReader = container.select(DataContentService.class).get();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() {
        if (container != null) {
            container.close();
        }
    }
}
