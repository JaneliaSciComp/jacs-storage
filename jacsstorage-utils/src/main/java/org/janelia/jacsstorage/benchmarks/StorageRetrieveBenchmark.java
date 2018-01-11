package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.utils.AuthClientImplHelper;
import org.janelia.jacsstorage.utils.StorageClientImplHelper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StorageRetrieveBenchmark {

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void listStorage(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        PageResult<DataStorageInfo> storageRecords = trialParams.storageClientHelper.listStorageRecords(trialParams.serverURL, invocationParams.storageBundleId, invocationParams.pageRequest, trialParams.authToken);
        for (DataStorageInfo storageInfo : storageRecords.getResultList()) {
            trialParams.storageClientHelper.listStorageContent(storageInfo.getConnectionURL(), storageInfo.getId(), trialParams.authToken);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput, Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamStorageContent(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams) {
        for (DataNodeInfo contentInfo : invocationParams.storageContent) {
            trialParams.storageClientHelper.streamDataEntryFromStorage(contentInfo.getRootLocation(), contentInfo.getStorageId(), contentInfo.getNodeRelativePath(), trialParams.authToken)
                .map(is -> {
                    try {
                        byte[] content = ByteStreams.toByteArray(is);
                        System.out.println("!!!!! READ " + content.length + " bytes");
                        return content;
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
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
        String authToken = new AuthClientImplHelper(benchmarksCmdLineParams.authURL).authenticate(benchmarksCmdLineParams.username, benchmarksCmdLineParams.password);
        String dataOwnerKey = benchmarksCmdLineParams.getUserKey();
        long nStorageRecords = new StorageClientImplHelper().countStorageRecords(benchmarksCmdLineParams.serverURL, benchmarksCmdLineParams.bundleId, authToken);
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks =  StorageRetrieveBenchmark.class.getSimpleName() + "\\." + benchmarksCmdLineParams.benchmarksRegex;
        } else {
            benchmarks =  StorageRetrieveBenchmark.class.getSimpleName();
        }
        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(benchmarksCmdLineParams.warmupIterations)
                .measurementIterations(benchmarksCmdLineParams.measurementIterations)
                .forks(benchmarksCmdLineParams.nForks)
                .threads(benchmarksCmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("serverURL", benchmarksCmdLineParams.serverURL)
                .param("useHttp", benchmarksCmdLineParams.useHttp.toString())
                .param("ownerKey", dataOwnerKey)
                .param("dataLocation", benchmarksCmdLineParams.localPath)
                .param("authToken", authToken)
                .param("nStorageRecords", String.valueOf(nStorageRecords))
                ;
        if (benchmarksCmdLineParams.bundleId != null) {
            optBuilder = optBuilder.param("dataBundleId", benchmarksCmdLineParams.bundleId.toString());
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
