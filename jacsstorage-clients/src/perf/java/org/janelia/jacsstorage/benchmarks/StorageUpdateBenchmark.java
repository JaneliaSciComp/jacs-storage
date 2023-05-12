package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.client.clientutils.AuthClientImplHelper;
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
    @OutputTimeUnit(TimeUnit.SECONDS)
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
    @OutputTimeUnit(TimeUnit.SECONDS)
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
        ClientBenchmarksCmdLineParams cmdLineParams = new ClientBenchmarksCmdLineParams();
        JCommander jc = JCommander.newBuilder()
                .addObject(cmdLineParams)
                .build();
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            jc.usage();
            System.exit(1);
        }
        String authToken = new AuthClientImplHelper(cmdLineParams.authURL).authenticate(cmdLineParams.username, cmdLineParams.password);
        String dataOwnerKey = cmdLineParams.getUserKey();
        String benchmarks;
        if (StringUtils.isNotBlank(cmdLineParams.benchmarksRegex)) {
            benchmarks =  StorageUpdateBenchmark.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks =  StorageUpdateBenchmark.class.getSimpleName();
        }
        Options opt = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(cmdLineParams.warmupIterations)
                .measurementIterations(cmdLineParams.measurementIterations)
                .forks(cmdLineParams.nForks)
                .threads(cmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("serverURL", cmdLineParams.serverURL)
                .param("ownerKey", dataOwnerKey)
                .param("dataLocation", cmdLineParams.localPath)
                .param("dataBundleId", cmdLineParams.bundleId.toString())
                .param("updatedDataPath", cmdLineParams.updatedPath)
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
