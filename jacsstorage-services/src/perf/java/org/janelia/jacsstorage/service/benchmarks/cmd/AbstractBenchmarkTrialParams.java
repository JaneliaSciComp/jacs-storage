package org.janelia.jacsstorage.service.benchmarks.cmd;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rng.UniformRandomProvider;
import org.apache.commons.rng.simple.RandomSource;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
abstract public class AbstractBenchmarkTrialParams {
    @Param({""})
    String s3EntriesFile;
    private List<String> s3Entries = new ArrayList<>();

    @Param({""})
    String accessKey;

    @Param({""})
    String secretKey;

    @Param({""})
    String s3Region;

    @Param("s3://")
    String s3URIPrefix;

    @Param("/")
    String s3fsMountPoint;

    private final UniformRandomProvider rng = RandomSource.create(RandomSource.XO_RO_SHI_RO_128_PP);

    @Setup(Level.Trial)
    public void loadTestEntries(BenchmarkParams params) {
        if (StringUtils.isNotBlank(s3EntriesFile)) {
            try {
                s3Entries = Files.readAllLines(Paths.get(s3EntriesFile)).stream()
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    public String getRandomS3Entry() {
        return s3Entries.isEmpty() ? null : s3Entries.get(rng.nextInt(s3Entries.size()));
    }

    public String getRandomFSEntry() {
        String s3Entry = getRandomS3Entry();
        if (s3Entry != null) {
            return s3Entry.replace(s3URIPrefix, s3fsMountPoint);
        } else {
            return null;
        }
    }
}
