package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
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

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamFSContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamContentImpl(trialParams, blackhole, trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamFSObjectContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamObjectContentImpl(trialParams, blackhole, trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3Content(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ObjectContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamObjectContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listFSContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        listContentImpl(trialParams, blackhole, () -> trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listS3Content(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        listContentImpl(trialParams, blackhole, () -> trialParams.getRandomS3Entry());
    }

    private void listContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, Supplier<String> entrySupplier) {
        String entry = entrySupplier.get();
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(entry, new JADEStorageOptions());
            try {
                ContentGetter contentGetter = trialParams.storageContentReader.getDataContent(
                        dataURI,
                        new ContentAccessParams().setMaxDepth(1)
                );
                List<ContentNode> contentNodes = contentGetter.getObjectsList();
                blackhole.consume(contentNodes);
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void streamContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(entry, new JADEStorageOptions());
            try (OutputStream targetStream = new NullOutputStream()) {
                ContentGetter contentGetter = trialParams.storageContentReader.getDataContent(dataURI, new ContentAccessParams());
                long nbytes = contentGetter.streamContent(targetStream);
                blackhole.consume(nbytes);
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void streamObjectContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(entry, new JADEStorageOptions());
            try (OutputStream targetStream = new NullOutputStream()) {
                ContentGetter contentGetter = trialParams.storageContentReader.getObjectContent(dataURI);
                long nbytes = contentGetter.streamContent(targetStream);
                blackhole.consume(nbytes);
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    public static void main(String[] args) throws RunnerException {
        BenchmarksCmdLineParams cmdLineParams = new BenchmarksCmdLineParams();
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
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName() + "\\." + cmdLineParams.benchmarksRegex;
        } else {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName();
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
                .param("fsEntriesFile", cmdLineParams.fsEntriesFile);
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
