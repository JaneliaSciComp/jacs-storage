package org.janelia.jacsstorage.service.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.sun.jna.ptr.NativeLongByReference;
import org.apache.commons.lang3.StringUtils;
import org.blosc.IBloscDll;
import org.blosc.JBlosc;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentException;
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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
    public void streamAndDecompressS3ObjectContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamAndDecompressObjectContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
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
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(
                    entry,
                    JADEOptions.create()
                            .setAccessKey(StringUtils.isNotBlank(trialParams.accessKey) ? trialParams.accessKey : null)
                            .setSecretKey(StringUtils.isNotBlank(trialParams.secretKey) ? trialParams.secretKey : null)
            );

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
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(
                    entry,
                    JADEOptions.create()
                            .setAccessKey(StringUtils.isNotBlank(trialParams.accessKey) ? trialParams.accessKey : null)
                            .setSecretKey(StringUtils.isNotBlank(trialParams.secretKey) ? trialParams.secretKey : null)
                            .setAWSRegion(trialParams.s3Region)
                            .setAsyncAccess(trialParams.useAsync)
            );
            try (OutputStream targetStream = new NullOutputStream()) {
                ContentGetter contentGetter = trialParams.storageContentReader.getDataContent(dataURI, new ContentAccessParams());
                long nbytes = contentGetter.streamContent(targetStream);
                if (nbytes == 0) {
                    throw new ContentException("Empty content " + dataURI);
                } else {
                    blackhole.consume(nbytes);
                }
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void streamObjectContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(
                    entry,
                    JADEOptions.create()
                            .setAccessKey(StringUtils.isNotBlank(trialParams.accessKey) ? trialParams.accessKey : null)
                            .setSecretKey(StringUtils.isNotBlank(trialParams.secretKey) ? trialParams.secretKey : null)
                            .setAWSRegion(trialParams.s3Region)
                            .setAsyncAccess(trialParams.useAsync)
            );
            try (OutputStream targetStream = new NullOutputStream()) {
                ContentGetter contentGetter = trialParams.storageContentReader.getObjectContent(dataURI);
                long nbytes = contentGetter.streamContent(targetStream);
                if (nbytes == 0) {
                    throw new ContentException("Empty content " + dataURI);
                } else {
                    blackhole.consume(nbytes);
                }
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void streamAndDecompressObjectContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(
                    entry,
                    JADEOptions.create()
                            .setAccessKey(StringUtils.isNotBlank(trialParams.accessKey) ? trialParams.accessKey : null)
                            .setSecretKey(StringUtils.isNotBlank(trialParams.secretKey) ? trialParams.secretKey : null)
                            .setAWSRegion(trialParams.s3Region)
                            .setAsyncAccess(trialParams.useAsync)
            );
            try (ByteArrayOutputStream targetStream = new ByteArrayOutputStream()) {
                ContentGetter contentGetter = trialParams.storageContentReader.getObjectContent(dataURI);
                long nbytes = contentGetter.streamContent(targetStream);
                if (nbytes == 0) {
                    throw new ContentException("Empty content " + dataURI);
                } else {
                    JBlosc jb = new JBlosc();
                    byte[] compressedData = targetStream.toByteArray();
                    ByteBuffer header = ByteBuffer.wrap(compressedData, 0, JBlosc.OVERHEAD);
                    NativeLongByReference nbytesValue = new NativeLongByReference();
                    NativeLongByReference cbytesValue = new NativeLongByReference();
                    NativeLongByReference blocksizeValue = new NativeLongByReference();

                    IBloscDll.blosc_cbuffer_sizes(header, nbytesValue, cbytesValue, blocksizeValue);
                    int compressedSize = cbytesValue.getValue().intValue();
                    int uncompressedSize = nbytesValue.getValue().intValue();
                    ByteBuffer compressedBuffer = ByteBuffer.wrap(compressedData);
                    ByteBuffer uncompressedBuffer = ByteBuffer.allocateDirect(uncompressedSize);
                    int s = jb.decompress(compressedBuffer, uncompressedBuffer, uncompressedSize);
                    assert s == uncompressedSize;
                    LOG.info("Blosc uncompress buffer {} -> {}", compressedSize, uncompressedSize);
                    blackhole.consume(s);
                }
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
                .param("fsEntriesFile", cmdLineParams.fsEntriesFile)
                .param("accessKey", cmdLineParams.accessKey)
                .param("secretKey", cmdLineParams.secretKey)
                .param("useAsync", Boolean.toString(cmdLineParams.useAsync))
                .param("s3Region", cmdLineParams.s3Region)
                ;
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
