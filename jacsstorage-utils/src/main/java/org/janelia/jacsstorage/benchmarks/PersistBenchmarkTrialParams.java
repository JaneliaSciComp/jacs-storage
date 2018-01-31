package org.janelia.jacsstorage.benchmarks;

import org.janelia.jacsstorage.client.StorageClient;
import org.janelia.jacsstorage.client.StorageClientHttpImpl;
import org.janelia.jacsstorage.datatransfer.DataTransferService;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;

@State(Scope.Benchmark)
public class PersistBenchmarkTrialParams extends BenchmarkTrialParams {

    StorageClient storageClient;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        super.setUp(params);
        SeContainerInitializer containerInit = SeContainerInitializer.newInstance();
        SeContainer container = containerInit.initialize();
        storageClient = new StorageClientHttpImpl(
                container.select(DataTransferService.class).get()
        );
    }

}
