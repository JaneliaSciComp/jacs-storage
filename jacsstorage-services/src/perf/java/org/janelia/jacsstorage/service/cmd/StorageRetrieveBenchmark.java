package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentGetter;
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
import java.util.concurrent.TimeUnit;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentAvg(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentImpl(trialParams, blackhole);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamPathContentThrpt(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamPathContentImpl(trialParams, blackhole);
    }

    private void streamPathContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        OutputStream targetStream = new NullOutputStream();
        JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(trialParams.getRandomEntry());
        try {
            ContentGetter contentGetter = trialParams.storageContentReader.getDataContent(dataURI, new ContentAccessParams());
            long nbytes = contentGetter.streamContent(targetStream);
            blackhole.consume(nbytes);
        } catch (Exception e) {
            LOG.error("Error reading {}", dataURI, e);
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
        String benchmarks;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.benchmarksRegex)) {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName() + "\\." + benchmarksCmdLineParams.benchmarksRegex;
        } else {
            benchmarks = StorageRetrieveBenchmark.class.getSimpleName();
        }

        ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(benchmarks)
                .warmupIterations(benchmarksCmdLineParams.warmupIterations)
                .warmupTime(benchmarksCmdLineParams.getWarmupTime())
                .measurementIterations(benchmarksCmdLineParams.measurementIterations)
                .measurementTime(benchmarksCmdLineParams.getMeasurementTime())
                .measurementBatchSize(benchmarksCmdLineParams.measurementBatchSize)
                .forks(benchmarksCmdLineParams.nForks)
                .threads(benchmarksCmdLineParams.nThreads)
                .shouldFailOnError(true)
                .detectJvmArgs()
                .param("entriesPathsFile", benchmarksCmdLineParams.entriesPathsFile)
                ;
        if (StringUtils.isNotBlank(benchmarksCmdLineParams.profilerName)) {
            optBuilder.addProfiler(benchmarksCmdLineParams.profilerName);
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
