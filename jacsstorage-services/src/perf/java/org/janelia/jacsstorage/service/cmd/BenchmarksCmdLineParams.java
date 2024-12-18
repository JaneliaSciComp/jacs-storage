package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.runner.options.TimeValue;

public class BenchmarksCmdLineParams {

    @Parameter(names = "--profiler", description = "Benchmark profiler name. " +
            "Valid profilers: {cl, comp, gc, hs_cl, hs_comp, hs_gc, hs_rt, hs_thr, stack}")
    public String profilerName;
    @Parameter(names = "--warmup", description = "Warmup iterations")
    public int warmupIterations = 1;
    @Parameter(names = "--measurements", description = "Measurement iterations")
    public int measurementIterations = 1;
    @Parameter(names = "--measurement-batch", description = "Measurement batch size")
    public int measurementBatchSize = 1;
    @Parameter(names = "--measurement-time", description = "Measurement time")
    public String measurementTime = "30s";
    @Parameter(names = "--warmup-time", description = "Warmup time")
    public String warmupTime = "10s";
    @Parameter(names = "--forks", description = "Number of process instances")
    public int nForks = 1;
    @Parameter(names = "--threads", description = "Number of threads")
    public int nThreads = 1;
    @Parameter(names = "--benchmarks-regex", description = "benchmarks to be run regex")
    public String benchmarksRegex;
    @Parameter(names = "--s3-entries-file", description = "File containing list of S3 URIs to retrieve")
    public String s3EntriesFile = "";
    @Parameter(names = "--fs-entries-file", description = "File containing list of file paths to retrieve")
    public String fsEntriesFile = "";

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
