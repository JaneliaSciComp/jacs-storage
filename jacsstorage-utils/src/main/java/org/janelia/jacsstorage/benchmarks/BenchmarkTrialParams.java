package org.janelia.jacsstorage.benchmarks;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class BenchmarkTrialParams {
    @Param({""})
    String serverURL;
    @Param({""})
    String owner;
    @Param({""})
    String dataLocation;
    @Param({""})
    String dataFormat;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        serverURL = params.getParam("serverURL");
        owner = params.getParam("owner");
        dataLocation = params.getParam("dataLocation");
        dataFormat = params.getParam("dataFormat");
    }
}