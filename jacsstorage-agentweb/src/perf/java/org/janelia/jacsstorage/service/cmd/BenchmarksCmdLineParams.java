package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.runner.options.TimeValue;

class BenchmarksCmdLineParams {

    @Parameter(names = "-profilerName", description = "Benchmark profiler name. " +
            "Valid profilers: {cl, comp, gc, hs_cl, hs_comp, hs_gc, hs_rt, hs_thr, stack}")
    public String profilerName;
    @Parameter(names = "-warmup", description = "Warmup iterations")
    int warmupIterations = 5;
    @Parameter(names = "-measurements", description = "Measurement iterations")
    int measurementIterations = 5;
    @Parameter(names = "-measurementBatch", description = "Measurement batch size")
    int measurementBatchSize = 1;
    @Parameter(names = "-measurementTime", description = "Measurement time")
    String measurementTime = "";
    @Parameter(names = "-warmupTime", description = "Warmup time")
    String warmupTime = "";
    @Parameter(names = "-forks", description = "Number of process instances")
    int nForks = 1;
    @Parameter(names = "-threads", description = "Number of threads")
    int nThreads = 5;
    @Parameter(names = "-benchmarksRegex", description = "benchmarks to be run regex")
    String benchmarksRegex;
    @Parameter(names = "-entriesFile", description = "File containing list of paths to retrieve")
    String entriesPathsFile = "";

    TimeValue getMeasurementTime() {
        if (StringUtils.isBlank(measurementTime)) {
            return TimeValue.NONE;
        } else {
            return TimeValue.fromString(measurementTime);
        }
    }

    TimeValue getWarmupTime() {
        if (StringUtils.isBlank(warmupTime)) {
            return TimeValue.NONE;
        } else {
            return TimeValue.fromString(warmupTime);
        }
    }
}
