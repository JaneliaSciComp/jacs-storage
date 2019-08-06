package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class UpdateBenchmarkInvocationParams {
    Number storageBundleId;
    String newPath;

    @Setup(Level.Invocation)
    public void setUp(BenchmarkTrialParams params) {
        storageBundleId = params.dataBundleId;
        if (StringUtils.isBlank(params.updatedDataPath)) {
            newPath = RandomStringUtils.randomAlphanumeric(6);
        } else {
            newPath = params.updatedDataPath + "/" + RandomStringUtils.randomAlphanumeric(6);
        }
    }
}
