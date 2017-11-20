package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.client.SocketStorageClient;
import org.janelia.jacsstorage.client.StorageClient;
import org.janelia.jacsstorage.client.StorageClientHttpImpl;
import org.janelia.jacsstorage.client.StorageClientImpl;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataTransferService;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

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
