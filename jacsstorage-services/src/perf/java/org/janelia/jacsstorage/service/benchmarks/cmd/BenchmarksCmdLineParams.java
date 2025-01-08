package org.janelia.jacsstorage.service.benchmarks.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.Param;
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
    @Parameter(names = "--benchmarks-regex", description = "benchmarks to be run regex (e.g. streamS3 or streamFS)")
    public String benchmarksRegex;
    @Parameter(names = "--s3-entries-file", description = "File containing list of S3 URIs to retrieve (required for streamS3 tests)")
    public String s3EntriesFile = "";
    @Parameter(names = "--s3fs-mountpoint", description = "s3fs mount point")
    String s3fsMountPoint = "";
    @Parameter(names = "--async", description = "Use async access", arity = 0)
    public boolean useAsync = false;
    @Parameter(names = "--access-key", description = "S3 access key")
    public String accessKey = "";
    @Parameter(names = "--secret-key", description = "S3 secret key")
    public String secretKey = "";
    @Parameter(names = "--s3-region", description = "S3 region")
    public String s3Region = "us-east-1";

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

    public String getS3FuseMountPoint() {
        return StringUtils.appendIfMissing(s3fsMountPoint, "/");
    }
}
