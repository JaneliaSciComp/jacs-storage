package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.client.clientutils.AuthClientImplHelper;
import org.janelia.jacsstorage.io.ContentAccessParams;
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
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void persist(PersistBenchmarkTrialParams trialParams, PersistBenchmarkInvocationParams invocationParams) throws Exception {
        trialParams.storageClient.persistData(trialParams.dataLocation, invocationParams.dataStorageInfo, new ContentAccessParams(), trialParams.authToken);
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
            benchmarks =  StoragePersistBenchmark.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks =  StoragePersistBenchmark.class.getSimpleName();
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
                .param("dataFormat", cmdLineParams.dataFormat.name())
                .param("storageHost", cmdLineParams.storageHost)
                .param("storageTags", cmdLineParams.getStorageTagsAsString())
                .param("storageContext", cmdLineParams.storageContext)
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
