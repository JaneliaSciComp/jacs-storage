package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ContainerResponse;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.PathBasedAgentStorageResource;
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
import org.openjdk.jmh.util.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamPathContentFutureAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFuture(trialParams, blackhole);
    }

    private void streamPathContentFuture(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        String dataEntry = trialParams.getRandomEntry();
        try {
            URI requestURI = UriBuilder.fromPath(Constants.AGENTSTORAGE_URI_PATH).path("storage_path").path(dataEntry)
                    .build(dataEntry);
            Future<ContainerResponse> responseFuture = trialParams.appHandler().apply(trialParams.request(requestURI, "GET"));
            ContainerResponse response = responseFuture.get();
            blackhole.consume(response.getStatus());
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
	        throw new IllegalStateException(e);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamPathContentAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamPathContentImpl(trialParams, blackhole);
    }

    private void streamPathContentImpl(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        String dataEntry = trialParams.getRandomEntry();
        try {
            URI requestURI = UriBuilder.fromMethod(PathBasedAgentStorageResource.class, "retrieveData").build(dataEntry);
            InputStream response = trialParams.getTarget(requestURI).request().get(InputStream.class);
            OutputStream targetStream = new NullOutputStream();
            long n = ByteStreams.copy(response, targetStream);
            blackhole.consume(n);
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
        }
    }

    public static void main(String[] args) throws RunnerException {
        BenchmarksCmdLineParams benchmarksCmdLineParams = new BenchmarksCmdLineParams();
        JCommander jc = JCommander.newBuilder()
                .addObject(benchmarksCmdLineParams)
                .build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            jc.usage();
            System.exit(1);
        }
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName() + "\\." + benchmarksCmdLineParams.benchmarksRegex;
        } else {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName();
        }

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(benchmarksCmdLineParams.warmupIterations)
                .warmupTime(benchmarksCmdLineParams.getWarmupTime())
                .measurementIterations(benchmarksCmdLineParams.measurementIterations)
                .measurementTime(benchmarksCmdLineParams.getMeasurementTime())
                .measurementBatchSize(benchmarksCmdLineParams.measurementBatchSize)
                .forks(benchmarksCmdLineParams.nForks)
                .threads(benchmarksCmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("entriesPathsFile", benchmarksCmdLineParams.entriesPathsFile)
                ;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.profilerName)) {
            optBuilder.addProfiler(benchmarksCmdLineParams.profilerName);
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
