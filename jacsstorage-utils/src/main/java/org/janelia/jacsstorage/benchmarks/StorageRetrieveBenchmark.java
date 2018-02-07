package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteStreams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
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

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void listStorageAvg(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        listStorageImpl(trialParams, invocationParams);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void listStorageThrpt(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        listStorageImpl(trialParams, invocationParams);
    }

    private void listStorageImpl(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        PageResult<DataStorageInfo> storageRecords = trialParams.storageClientHelper.listStorageRecords(trialParams.serverURL, trialParams.storageHost, trialParams.getStorageTags(), invocationParams.storageBundleId, invocationParams.pageRequest, trialParams.authToken);
        for (DataStorageInfo storageInfo : storageRecords.getResultList()) {
            trialParams.storageClientHelper.listStorageContent(storageInfo.getConnectionURL(), storageInfo.getNumericId(), trialParams.authToken);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamStorageContentEntryAvg(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentEntryImpl(trialParams, invocationParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamStorageContentEntryThrpt(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentEntryImpl(trialParams, invocationParams, blackhole);
    }

    private void streamStorageContentEntryImpl(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        DataNodeInfo contentInfo = invocationParams.storageContent.get(RandomUtils.nextInt(0, CollectionUtils.size(invocationParams.storageContent)));
        OutputStream targetStream = new NullOutputStream();
        long nbytes = trialParams.storageClientHelper.streamDataEntryFromStorage(contentInfo.getRootLocation(), contentInfo.getStorageId(), contentInfo.getNodeRelativePath(), trialParams.authToken)
                .map(is -> {
                    try {
                        return ByteStreams.copy(is, targetStream);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamStorageContentAvg(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentImpl(trialParams, invocationParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void streamStorageContentThrpt(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentImpl(trialParams, invocationParams, blackhole);
    }

    private void streamStorageContentImpl(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        DataNodeInfo contentInfo = invocationParams.storageContent.get(RandomUtils.nextInt(0, CollectionUtils.size(invocationParams.storageContent)));
        OutputStream targetStream = new NullOutputStream();
        long nbytes = trialParams.storageClientHelper.streamDataFromStore(contentInfo.getRootLocation(), contentInfo.getStorageId(), trialParams.authToken)
                .map(is -> {
                    try {
                        if (StringUtils.isBlank(trialParams.dataLocation)) {
                            return ByteStreams.copy(is, targetStream);
                        } else {
                            Path dataLocation = getTempDataLocation(trialParams);
                            Files.createDirectories(dataLocation.getParent());
                            return Files.copy(is, dataLocation);
                        }
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    private Path getTempDataLocation(RetrieveBenchmarkTrialParams trialParams) {
        return Paths.get(trialParams.dataLocation, String.valueOf(RandomUtils.nextLong(0, Integer.MAX_VALUE)));
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
        System.out.println("AuthToken: " + authToken);
        String dataOwnerKey = benchmarksCmdLineParams.getUserKey();
        long nStorageRecords = new StorageClientImplHelper().countStorageRecords(benchmarksCmdLineParams.serverURL, benchmarksCmdLineParams.bundleId, authToken);
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName() + "\\." + benchmarksCmdLineParams.benchmarksRegex;
        } else {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName();
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
                .param("ownerKey", dataOwnerKey)
                .param("storageHost", StringUtils.defaultIfBlank(benchmarksCmdLineParams.storageHost, ""))
                .param("storageTags", benchmarksCmdLineParams.getStorageTagsAsString())
                .param("dataLocation", benchmarksCmdLineParams.localPath)
                .param("dataBundleId", benchmarksCmdLineParams.bundleId.toString())
                .param("storageEntry", benchmarksCmdLineParams.entryName)
                .param("authToken", authToken)
                .param("nStorageRecords", String.valueOf(nStorageRecords));
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
