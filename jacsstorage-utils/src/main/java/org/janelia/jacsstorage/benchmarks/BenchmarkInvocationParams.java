package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomStringUtils;
import org.janelia.jacsstorage.client.SocketStorageClient;
import org.janelia.jacsstorage.client.StorageClient;
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
public class BenchmarkInvocationParams {
    StorageClient storageClient;
    DataStorageInfo dataStorageInfo;

    @Setup(Level.Invocation)
    public void setUp(BenchmarkTrialParams params) {
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();

        storageClient = new StorageClientImpl(
                new SocketStorageClient(
                    container.select(DataTransferService.class).get()
                )
        );
        dataStorageInfo = new DataStorageInfo()
                .setConnectionInfo(params.serverURL)
                .setStorageFormat(JacsStorageFormat.valueOf(params.dataFormat))
                .setOwner(params.owner)
                .setName(RandomStringUtils.randomAlphanumeric(10) + "-" + System.nanoTime())
                ;
    }
}
