package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.runner.options.TimeValue;

public class BenchmarksCmdLineParams {

    @Parameter(names = "-profilerName", description = "Benchmark profiler name. " +
            "Valid profilers: {cl, comp, gc, hs_cl, hs_comp, hs_gc, hs_rt, hs_thr, stack}")
    public String profilerName;
    @Parameter(names = "-warmup", description = "Warmup iterations")
    public int warmupIterations = 5;
    @Parameter(names = "-measurements", description = "Measurement iterations")
    public int measurementIterations = 5;
    @Parameter(names = "-measurementBatch", description = "Measurement batch size")
    public int measurementBatchSize = 1;
    @Parameter(names = "-measurementTime", description = "Measurement time")
    public String measurementTime = "";
    @Parameter(names = "-warmupTime", description = "Warmup time")
    public String warmupTime = "";
    @Parameter(names = "-forks", description = "Number of process instances")
    public int nForks = 1;
    @Parameter(names = "-threads", description = "Number of threads")
    public int nThreads = 5;
    @Parameter(names = "-benchmarksRegex", description = "benchmarks to be run regex")
    public String benchmarksRegex;
    @Parameter(names = "-entriesFile", description = "File containing list of paths to retrieve")
    public String entriesPathsFile = "";

    public TimeValue getMeasurementTime() {
        if (StringUtils.isBlank(measurementTime)) {
            return TimeValue.NONE;
        } else {
            return TimeValue.fromString(measurementTime);
        }
    }

    public TimeValue getWarmupTime() {
        if (StringUtils.isBlank(warmupTime)) {
            return TimeValue.NONE;
        } else {
            return TimeValue.fromString(warmupTime);
        }
    }
}
