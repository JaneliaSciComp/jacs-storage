package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomUtils;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

@State(Scope.Thread)
public class ListContentBenchmarkInvocationParams {
    Number storageBundleId;
    PageRequest pageRequest;

    @Setup(Level.Invocation)
    public void setUp(RetrieveBenchmarkTrialParams params) {
        storageBundleId = params.dataBundleId;
        PageRequestBuilder pageRequestBuilder = new PageRequestBuilder().pageSize(1);
        if (params.nStorageRecords > 0) {
            pageRequestBuilder.pageNumber(RandomUtils.nextLong(0, params.nStorageRecords));
        }
        pageRequest = pageRequestBuilder.build();
    }
}
