package org.janelia.jacsstorage.agent.benchmarks.cmd;

import java.io.OutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.ws.rs.core.UriBuilder;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
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

public class AgentAppBenchmarks {

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamFSContentFromAbsolutePath(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamContentFromAbsolutePath(trialParams, trialParams.getRandomFSEntry(), blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ContentFromAbsolutePath(RetrieveBenchmarkResourceTrialParams trialParams, Blackhole blackhole) {
        streamContentFromAbsolutePath(trialParams, trialParams.getRandomS3Entry(), blackhole);
    }

    private void streamContentFromAbsolutePath(RetrieveBenchmarkResourceTrialParams trialParams, String dataEntry, Blackhole blackhole) {
        try {
            Future<ContainerResponse> responseFuture = streamPathContentFuture(trialParams, dataEntry);
            ContainerResponse response = responseFuture.get();
            blackhole.consume(response.getStatus());
            OutputStream targetStream = new NullOutputStream();
            response.setEntityStream(targetStream);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private Future<ContainerResponse> streamPathContentFuture(RetrieveBenchmarkResourceTrialParams trialParams, String dataEntry) {
        try {
            URI requestURI = UriBuilder
                    .fromUri(trialParams.createBaseURI())
                    .path("agent_storage/storage_path/data_content")
                    .path(dataEntry)
                    .build();
            ContainerRequest request = trialParams.request(requestURI, "GET");
            return trialParams.appHandler()
                    .apply(request);
        } catch (Exception e) {
	        throw new IllegalStateException(e);
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
                .param("s3fsMountPoint", cmdLineParams.getS3FuseMountPoint())
                .param("storageAgentURL", cmdLineParams.storageAgentURL)
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
