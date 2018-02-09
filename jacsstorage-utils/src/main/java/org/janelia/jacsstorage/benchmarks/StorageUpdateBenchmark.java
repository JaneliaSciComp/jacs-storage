package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.clientutils.AuthClientImplHelper;
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

import java.io.FileInputStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StorageUpdateBenchmark {

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void createDir(BenchmarkTrialParams trialParams, UpdateBenchmarkInvocationParams invocationParams) throws Exception {
        trialParams.storageClientHelper.createNewDirectory(
                trialParams.serverURL,
                invocationParams.storageBundleId,
                invocationParams.newPath,
                trialParams.authToken
        );
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void createNewFile(BenchmarkTrialParams trialParams, UpdateBenchmarkInvocationParams invocationParams) throws Exception {
        trialParams.storageClientHelper.createNewFile(
                trialParams.serverURL,
                invocationParams.storageBundleId,
                invocationParams.newPath,
                new FileInputStream(trialParams.dataLocation),
                trialParams.authToken
        );
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
        String authToken = new AuthClientImplHelper(benchmarksCmdLineParams.authURL).authenticate(benchmarksCmdLineParams.username, benchmarksCmdLineParams.password);
        String dataOwnerKey = benchmarksCmdLineParams.getUserKey();
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks =  StorageUpdateBenchmark.class.getSimpleName() + "\\." + benchmarksCmdLineParams.benchmarksRegex;
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
                .param("ownerKey", dataOwnerKey)
                .param("dataLocation", benchmarksCmdLineParams.localPath)
                .param("dataBundleId", benchmarksCmdLineParams.bundleId.toString())
                .param("updatedDataPath", benchmarksCmdLineParams.updatedPath)
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
