package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomStringUtils;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class PersistBenchmarkInvocationParams {
    DataStorageInfo dataStorageInfo;

    @Setup(Level.Invocation)
    public void setUp(BenchmarkTrialParams params) {
        dataStorageInfo = new DataStorageInfo()
                .setConnectionURL(params.serverURL)
                .setStorageFormat(JacsStorageFormat.valueOf(params.dataFormat))
                .setOwnerKey(params.ownerKey)
                .setPath(params.storageContext)
                .setStorageTags(params.getStorageTags())
                .setName(RandomStringUtils.randomAlphanumeric(10) + "-" + System.nanoTime())
                ;
    }
}
