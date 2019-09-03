package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteStreams;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.clientutils.AuthClientImplHelper;
import org.janelia.jacsstorage.clientutils.StorageClientImplHelper;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageResult;
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
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.util.NullOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.InvocationCallback;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listStorageAvg(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        listStorageImpl(trialParams, invocationParams);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listStorageThrpt(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        listStorageImpl(trialParams, invocationParams);
    }

    private void listStorageImpl(RetrieveBenchmarkTrialParams trialParams, ListContentBenchmarkInvocationParams invocationParams) {
        PageResult<DataStorageInfo> storageRecords = trialParams.storageClientHelper.listStorageRecords(trialParams.serverURL, trialParams.storageAgentId, trialParams.getStorageTags(), invocationParams.storageBundleId, invocationParams.pageRequest, trialParams.authToken);
        for (DataStorageInfo storageInfo : storageRecords.getResultList()) {
            trialParams.storageClientHelper.listStorageContent(storageInfo.getConnectionURL(), storageInfo.getNumericId(), trialParams.authToken);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamStorageContentEntryAvg(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentEntryImpl(trialParams, invocationParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamStorageContentEntryThrpt(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentEntryImpl(trialParams, invocationParams, blackhole);
    }

    private void streamStorageContentEntryImpl(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        DataNodeInfo contentInfo = invocationParams.storageContent.get(RandomUtils.nextInt(0, CollectionUtils.size(invocationParams.storageContent)));
        OutputStream targetStream = new NullOutputStream();
        long nbytes = trialParams.storageClientHelper.streamDataEntryFromStorage(contentInfo.getStorageRootLocation(), contentInfo.getNumericStorageId(), contentInfo.getNodeRelativePath(), trialParams.authToken)
                .map(is -> {
                    try {
                        return ByteStreams.copy(is, targetStream);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ignore) {
                            // ignore
                        }
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamStorageContentAvg(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentImpl(trialParams, invocationParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamStorageContentThrpt(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        streamStorageContentImpl(trialParams, invocationParams, blackhole);
    }

    private void streamStorageContentImpl(RetrieveBenchmarkTrialParams trialParams, StreamContentBenchmarkInvocationParams invocationParams, Blackhole blackhole) {
        DataNodeInfo contentInfo = invocationParams.storageContent.get(RandomUtils.nextInt(0, CollectionUtils.size(invocationParams.storageContent)));
        OutputStream targetStream = new NullOutputStream();
        long nbytes = trialParams.storageClientHelper.streamDataFromStore(contentInfo.getStorageRootLocation(), contentInfo.getNumericStorageId(), trialParams.authToken)
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
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ignore) {
                            // ignore
                        }
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    private Path getTempDataLocation(RetrieveBenchmarkTrialParams trialParams) {
        return Paths.get(trialParams.dataLocation, String.valueOf(RandomUtils.nextLong(0, Integer.MAX_VALUE)));
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromAgentAvg(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromAgentImpl(trialParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromAgentThrpt(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromAgentImpl(trialParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromAgentSampleTime(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromAgentImpl(trialParams, blackhole);
    }

    private void streamPathContentFromAgentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        OutputStream targetStream = new NullOutputStream();
        String entry = trialParams.getRandomEntry();
        long nbytes = trialParams.storageClientHelper.streamPathContentFromAgent(trialParams.agentURL, entry, trialParams.authToken)
                .map(is -> {
                    try {
                        if (StringUtils.isBlank(trialParams.dataLocation)) {
                            return ByteStreams.copy(is, targetStream);
                        } else {
                            Path dataLocation = getTempDataLocation(trialParams);
                            Files.createDirectories(dataLocation.getParent());
                            return Files.copy(is, dataLocation);
                        }
                    } catch (Exception e) {
                        LOG.error("Error retrieving {}", entry, e);
                        return -1L;
                    } finally {
                        try {
                            is.close();
                        } catch (Exception ignore) {
                        }
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromMasterAvg(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromMasterImpl(trialParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromMasterThrpt(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromMasterImpl(trialParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.SampleTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentFromMasterSampleTime(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentFromMasterImpl(trialParams, blackhole);
    }

    private void streamPathContentFromMasterImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        OutputStream targetStream = new NullOutputStream();
        String entry = trialParams.getRandomEntry();
        long nbytes = trialParams.storageClientHelper.streamPathContentFromMaster(trialParams.serverURL, entry, trialParams.authToken)
                .map(is -> {
                    try {
                        if (StringUtils.isBlank(trialParams.dataLocation)) {
                            return ByteStreams.copy(is, targetStream);
                        } else {
                            Path dataLocation = getTempDataLocation(trialParams);
                            Files.createDirectories(dataLocation.getParent());
                            return Files.copy(is, dataLocation);
                        }
                    } catch (Exception e) {
                        LOG.error("Error retrieving {}", entry, e);
                        return -1L;
                    } finally {
                        try {
                            is.close();
                        } catch (Exception ignore) {
                        }
                    }
                })
                .orElse(0L);
        blackhole.consume(nbytes);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void checkPathContentFromMasterAvg(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        boolean bresult = trialParams.storageClientHelper.checkPathContentFromMaster(trialParams.serverURL, trialParams.getRandomEntry(), trialParams.authToken);
        blackhole.consume(bresult);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamAsyncPathContentFromMasterAvg(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        OutputStream targetStream = new NullOutputStream();
        String dataPath = trialParams.getRandomEntry();
        Future<InputStream> streamFuture = trialParams.storageClientHelper.asyncStreamPathContentFromMaster(trialParams.serverURL,
                dataPath,
                trialParams.authToken,
                new InvocationCallback<InputStream>() {
                    @Override
                    public void completed(InputStream is) {
                        // Completed
                        try {
                            long nBytes = ByteStreams.copy(is, targetStream);
                            blackhole.consume(nBytes);
                        } catch (Exception closeExc) {
                            LOG.error("Error closing stream for {}", dataPath, closeExc);
                        } finally {
                            try {
                                is.close();
                            } catch (IOException ignore) {
                            }
                        }
                    }

                    @Override
                    public void failed(Throwable failureExc) {
                        LOG.error("Error retrieving {}", dataPath, failureExc);
                    }
                });
        blackhole.consume(streamFuture);
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
        String authToken = "";
        if (StringUtils.isNotBlank(cmdLineParams.username)) {
            authToken = new AuthClientImplHelper(cmdLineParams.authURL).authenticate(cmdLineParams.username, cmdLineParams.password);
            System.out.println("AuthToken: " + authToken);
        } else {
            System.out.println("No Auth");
        }
        String dataOwnerKey = cmdLineParams.getUserKey();
        long nStorageRecords = new StorageClientImplHelper("Benchmarks").countStorageRecords(cmdLineParams.serverURL, cmdLineParams.bundleId, authToken);
        String benchmarks;
        if (StringUtils.isNotBlank(cmdLineParams.benchmarksRegex)) {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName();
        }
        System.out.println("include " + benchmarks);
        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .verbosity(VerboseMode.EXTRA)
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
                .param("serverURL", cmdLineParams.serverURL)
                .param("agentURL", cmdLineParams.agentURL)
                .param("ownerKey", dataOwnerKey)
                .param("storageHost", StringUtils.defaultIfBlank(cmdLineParams.storageHost, ""))
                .param("storageTags", cmdLineParams.getStorageTagsAsString())
                .param("dataLocation", cmdLineParams.localPath)
                .param("dataBundleId", cmdLineParams.bundleId.toString())
                .param("storageEntry", cmdLineParams.entryName)
                .param("entriesPathsFile", cmdLineParams.entriesPathsFile)
                .param("authToken", authToken)
                .param("nStorageRecords", String.valueOf(nStorageRecords));
        if (cmdLineParams.bundleId != null) {
            optBuilder = optBuilder.param("dataBundleId", cmdLineParams.bundleId.toString());
        }
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
