package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.utils.StorageClientImplHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StoragePersistBenchmark {

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void persistThroughput(BenchmarkTrialParams trialParams, PersistBenchmarkInvocationParams invocationParams) throws Exception {
        invocationParams.storageClient.persistData(trialParams.dataLocation, invocationParams.dataStorageInfo, trialParams.authToken);
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void persistAverageTime(BenchmarkTrialParams trialParams, PersistBenchmarkInvocationParams invocationParams) throws Exception {
        invocationParams.storageClient.persistData(trialParams.dataLocation, invocationParams.dataStorageInfo, trialParams.authToken);
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
        String authToken = new StorageClientImplHelper().authenticate(benchmarksCmdLineParams.username, benchmarksCmdLineParams.password);
        String dataOwner = benchmarksCmdLineParams.username;
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks =  StorageUpdateBenchmark.class.getSimpleName() + "." + benchmarksCmdLineParams.benchmarksRegex;
        } else {
            benchmarks =  StorageUpdateBenchmark.class.getSimpleName();
        }
        Options opt = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(benchmarksCmdLineParams.warmupIterations)
                .measurementIterations(benchmarksCmdLineParams.measurementIterations)
                .forks(benchmarksCmdLineParams.nForks)
                .threads(benchmarksCmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("serverURL", benchmarksCmdLineParams.serverURL)
                .param("useHttp", benchmarksCmdLineParams.useHttp.toString())
                .param("owner", dataOwner)
                .param("dataLocation", benchmarksCmdLineParams.localPath)
                .param("dataFormat", benchmarksCmdLineParams.dataFormat.name())
                .param("authToken", authToken)
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