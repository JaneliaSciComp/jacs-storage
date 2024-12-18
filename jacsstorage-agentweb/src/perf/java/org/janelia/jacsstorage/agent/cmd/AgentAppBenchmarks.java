package org.janelia.jacsstorage.agent.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ContainerResponse;
import org.janelia.jacsstorage.service.cmd.BenchmarksCmdLineParams;
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

import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AgentAppBenchmarks {

    private static final Logger LOG = LoggerFactory.getLogger(AgentAppBenchmarks.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ContentFutureAvg(RetrieveBenchmarkResourceTrialParams trialParams) {
        streamPathContentFuture(trialParams.getRandomS3Entry(), trialParams);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ContentFromFutureAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromFuture(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    private Future<ContainerResponse> streamPathContentFuture(String dataEntry, RetrieveBenchmarkResourceTrialParams trialParams) {
        try {
            URI requestURI = UriBuilder.fromPath("/agent_storage").path("storage_path").path(dataEntry)
                    .build(dataEntry);
            return trialParams.appHandler().apply(trialParams.request(requestURI, "GET"));
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
	        throw new IllegalStateException(e);
        }
    }

    private void streamPathContentFromFuture(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole, String dataEntry) {
        try {
            Future<ContainerResponse> responseFuture = streamPathContentFuture(dataEntry, trialParams);
            ContainerResponse response = responseFuture.get();
            blackhole.consume(response.getStatus());
            StreamingOutput responseStream = (StreamingOutput) response.getEntity();
            OutputStream targetStream = new NullOutputStream();
            responseStream.write(targetStream);
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
            throw new IllegalStateException(e);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ContentAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    private void streamContentImpl(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole, String dataEntry) {
        try {
            InputStream response = trialParams.target()
                    .path("/agent_storage")
                    .path("storage_path")
                    .path(dataEntry)
                    .request().get(InputStream.class);
            OutputStream targetStream = new NullOutputStream();
            long n = ByteStreams.copy(response, targetStream);
            blackhole.consume(n);
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ContentFromVolumePathAvg(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        String dataEntry = trialParams.getRandomS3Entry();
        try {
            InputStream response = trialParams.target()
                    .path("/agent_storage")
                    .path("storage_volume")
                    .path(trialParams.storageVolumeId)
                    .path("data_content")
                    .path(dataEntry)
                    .request().get(InputStream.class);
            OutputStream targetStream = new NullOutputStream();
            long n = ByteStreams.copy(response, targetStream);
            blackhole.consume(n);
        } catch (Exception e) {
            LOG.error("Error reading {}", dataEntry, e);
        }

    }

    public static void main(String[] args) throws RunnerException {
        AgentBenchmarksCmdLineParams cmdLineParams = new AgentBenchmarksCmdLineParams();
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
            benchmarks = AgentAppBenchmarks.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks = AgentAppBenchmarks.class.getSimpleName();
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
                .param("fsEntriesFile", cmdLineParams.fsEntriesFile)
                .param("storageVolumeId", cmdLineParams.storageVolumeId)
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
