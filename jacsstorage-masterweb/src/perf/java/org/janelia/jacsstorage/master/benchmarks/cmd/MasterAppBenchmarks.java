package org.janelia.jacsstorage.master.benchmarks.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ContainerResponse;
import org.janelia.jacsstorage.service.benchmarks.cmd.BenchmarksCmdLineParams;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.core.UriBuilder;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MasterAppBenchmarks {

    private static final Logger LOG = LoggerFactory.getLogger(MasterAppBenchmarks.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void redirectForS3ContentFutureAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        try {
            Future<ContainerResponse> responseFuture = redirectPathContentFuture(trialParams.getRandomS3Entry(), trialParams);
            ContainerResponse response = responseFuture.get();
            blackhole.consume(response.getStatus());
            blackhole.consume(response.getHeaderString("Location"));
        } catch (Exception e) {
            LOG.error("Error redirecting the request", e);
            throw new IllegalStateException(e);
        }
    }

    private Future<ContainerResponse> redirectPathContentFuture(String dataEntry, RetrieveBenchmarkResourceTrialParams trialParams) {
        URI requestURI = UriBuilder.fromPath("/storage_content").path("storage_path_redirect").queryParam("contentPath", dataEntry)
                .build(dataEntry);
        try {
            return trialParams.appHandler().apply(trialParams.request(requestURI, "GET"));
        } catch (Exception e) {
            LOG.error("Error while redirecting {} for {}", requestURI, dataEntry, e);
	        throw new IllegalStateException(e);
        }
    }

    public static void main(String[] args) throws RunnerException {
        BenchmarksCmdLineParams cmdLineParams = new BenchmarksCmdLineParams();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdLineParams)
                .build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            jc.usage();
            System.exit(1);
        }
        String benchmarks;
        if (StringUtils.isNotBlank(cmdLineParams.benchmarksRegex)) {
            benchmarks = MasterAppBenchmarks.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks = MasterAppBenchmarks.class.getSimpleName();
        }

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(cmdLineParams.warmupIterations)
                .warmupTime(cmdLineParams.getWarmupTime())
                .measurementIterations(cmdLineParams.measurementIterations)
                .measurementTime(cmdLineParams.getMeasurementTime())
                .measurementBatchSize(cmdLineParams.measurementBatchSize)
                .forks(cmdLineParams.nForks)
                .threads(cmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("s3EntriesFile", cmdLineParams.s3EntriesFile)
                .param("s3fsMountPoint", cmdLineParams.getS3FuseMountPoint())
                ;
        if (StringUtils.isNotBlank(cmdLineParams.profilerName)) {
            optBuilder.addProfiler(cmdLineParams.profilerName);
        }

        Options opt = optBuilder.build();

        Collection<RunResult> runResults = new Runner(opt).run();
        for (RunResult runResult : runResults) {
            Result result = runResult.getAggregatedResult().getPrimaryResult();
            System.out.println("Score: " + result.getScore() + " " +
                    result.getScoreUnit() + " over " +
                    result.getStatistics());
        }
    }
}
