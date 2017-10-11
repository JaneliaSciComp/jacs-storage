package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.RandomStringUtils;
import org.janelia.jacsstorage.client.SocketStorageClient;
import org.janelia.jacsstorage.client.StorageClient;
import org.janelia.jacsstorage.client.StorageClientImpl;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataTransferService;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StorageBenchmarks {

    private static class BenchmarksCmdLineParams {
        @Parameter(names = "-warmup", description = "Warmup iterations")
        private int warmupIterations = 5;
        @Parameter(names = "-measurements", description = "Measurement iterations")
        private int measurementIterations = 5;
        @Parameter(names = "-forks", description = "Number of process instances")
        private int nForks = 1;
        @Parameter(names = "-threads", description = "Number of threads")
        private int nThreads = 5;
        @Parameter(names = "-server", description = "Master storage server URL")
        private String serverURL = "http://jdcu1:8080/jacsstorage/master-api";
        @Parameter(names = "-dataFormat", description = "Data bundle format")
        private JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
        @Parameter(names = "-owner", description = "Data bundle owner")
        protected String owner = "benchmarks";
        @Parameter(names = "-localPath", description = "Local path")
        protected String localPath = "tt";
    }

    @State(Scope.Benchmark)
    public static class TrialParams {
        @Param({""})
        private String serverURL;
        @Param({""})
        private String owner;
        @Param({""})
        private String dataLocation;
        @Param({""})
        private String dataFormat;

        @Setup(Level.Trial)
        public void setUp(BenchmarkParams params) {
            serverURL = params.getParam("serverURL");
            owner = params.getParam("owner");
            dataLocation = params.getParam("dataLocation");
            dataFormat = params.getParam("dataFormat");
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkInvocationParams {
        private StorageClient storageClient;
        private DataStorageInfo dataStorageInfo;

        @Setup(Level.Invocation)
        public void setUp(TrialParams params) {
            SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
            SeContainer container = containerInit.initialize();

            storageClient = new StorageClientImpl(
                    new SocketStorageClient(
                        container.select(DataTransferService.class).get()
                    )
            );
            dataStorageInfo = new DataStorageInfo()
                    .setConnectionInfo(params.serverURL)
                    .setStorageFormat(JacsStorageFormat.valueOf(params.dataFormat))
                    .setOwner(params.owner)
                    .setName(RandomStringUtils.randomAlphanumeric(10))
                    ;
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureAvgPersistTime(TrialParams trialParams, BenchmarkInvocationParams invocationParams) throws Exception {
        invocationParams.storageClient.persistData(trialParams.dataLocation, invocationParams.dataStorageInfo);
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

        Options opt = new OptionsBuilder()
                .include(StorageBenchmarks.class.getSimpleName())
                .warmupIterations(benchmarksCmdLineParams.warmupIterations)
                .measurementIterations(benchmarksCmdLineParams.measurementIterations)
                .forks(benchmarksCmdLineParams.nForks)
                .threads(benchmarksCmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("serverURL", benchmarksCmdLineParams.serverURL)
                .param("owner", benchmarksCmdLineParams.owner)
                .param("dataLocation", benchmarksCmdLineParams.localPath)
                .param("dataFormat", benchmarksCmdLineParams.dataFormat.name())
                .build();

        Collection<RunResult> runResults = new Runner(opt).run();
        for (RunResult runResult : runResults) {
            Result result = runResult.getAggregatedResult().getPrimaryResult();
            System.out.println("Score: " + result.getScore() + " " +
                result.getScoreUnit() + " over " +
                result.getStatistics());
        }
    }
}
