package org.janelia.jacsstorage.service.benchmarks.cmd;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.sun.jna.ptr.NativeLongByReference;
import org.apache.commons.lang3.StringUtils;
import org.blosc.IBloscDll;
import org.blosc.JBlosc;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentAccessParams;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageRetrieveBenchmark {

    private static final Logger LOG = LoggerFactory.getLogger(StorageRetrieveBenchmark.class);

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamFSDataContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamWithGetDataContentImpl(trialParams, blackhole, trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamFSObjectContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamWithGetObjectContentImpl(trialParams, blackhole, trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3DataContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamWithGetDataContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void streamS3ObjectContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        streamWithGetObjectContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listFSContent(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        listContentImpl(trialParams, blackhole, trialParams.getRandomFSEntry());
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void listS3Content(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole) {
        listContentImpl(trialParams, blackhole, trialParams.getRandomS3Entry());
    }

    private void listContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
        if (StringUtils.isNotBlank(entry)) {
            JADEStorageURI dataURI = JADEStorageURI.createStoragePathURI(
                    entry,
                    JADEOptions.create()
                            .setAccessKey(StringUtils.isNotBlank(trialParams.accessKey) ? trialParams.accessKey : null)
                            .setSecretKey(StringUtils.isNotBlank(trialParams.secretKey) ? trialParams.secretKey : null)
            );

            try {
                LOG.info("List content from {}", dataURI);
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

    private void streamWithGetObjectContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
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
                LOG.info("Get object content from {}", dataURI);
                ContentGetter contentGetter = trialParams.storageContentReader.getObjectContent(dataURI);
                LOG.info("Found {} objects", contentGetter.getObjectsList().size());
                long nbytes = contentGetter.streamContent(targetStream);
                if (nbytes == 0) {
                    throw new ContentException("Empty content " + dataURI);
                } else {
                    if (trialParams.applyBloscDecompression) {
                        consumeUncompressedContent(targetStream.toByteArray(), blackhole);
                    } else {
                        blackhole.consume(nbytes);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void streamWithGetDataContentImpl(RetrieveBenchmarkTrialParams trialParams, Blackhole blackhole, String entry) {
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
                LOG.info("Get data content from {}", dataURI);
                ContentGetter contentGetter = trialParams.storageContentReader.getDataContent(dataURI, new ContentAccessParams());
                LOG.info("Found {} objects at {}", contentGetter.getObjectsList().size(), dataURI);
                long nbytes = contentGetter.streamContent(targetStream);
                if (nbytes == 0) {
                    throw new ContentException("Empty content " + dataURI);
                } else {
                    if (trialParams.applyBloscDecompression) {
                        consumeUncompressedContent(targetStream.toByteArray(), blackhole);
                    } else {
                        blackhole.consume(nbytes);
                    }
                }
            } catch (Exception e) {
                LOG.error("Error reading {}", dataURI, e);
            }
        }
    }

    private void consumeUncompressedContent(byte[] compressedData, Blackhole blackhole) {
        JBlosc jb = new JBlosc();
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
                .param("s3fsMountPoint", cmdLineParams.getS3FuseMountPoint())
                .param("s3URIPrefix", cmdLineParams.s3URIPrefix)
                .param("accessKey", cmdLineParams.accessKey)
                .param("secretKey", cmdLineParams.secretKey)
                .param("useAsync", Boolean.toString(cmdLineParams.useAsync))
                .param("s3Region", cmdLineParams.s3Region)
                .param("applyBloscDecompression", Boolean.toString(cmdLineParams.applyBloscDecompression))
                ;
        if (StringUtils.isNotBlank(cmdLineParams.profilerName)) {
            optBuilder.addProfiler(cmdLineParams.profilerName);
        }

        Options opt = optBuilder.build();

        Collection<RunResult> runResults = new Runner(opt).run();
        for (RunResult runResult : runResults) {
            Result<?> result = runResult.getAggregatedResult().getPrimaryResult();
            System.out.println("Score: " + result.getScore() + " " +
                    result.getScoreUnit() + " over " +
                    result.getStatistics());
        }
    }
}
