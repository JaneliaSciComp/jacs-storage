package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.List;
import java.util.stream.Collectors;

@State(Scope.Thread)
public class StreamContentBenchmarkInvocationParams {
    Number storageBundleId;
    PageRequest pageRequest;
    List<DataNodeInfo> storageContent;

    @Setup(Level.Invocation)
    public void setUp(RetrieveBenchmarkTrialParams params) {
        storageBundleId = params.dataBundleId;
        PageRequestBuilder pageRequestBuilder = new PageRequestBuilder().pageSize(1);
        if (params.nStorageRecords > 0) {
            pageRequestBuilder.pageNumber(RandomUtils.nextLong(0, params.nStorageRecords));
        }
        pageRequest = pageRequestBuilder.build();
        PageResult<DataStorageInfo> storageRecords = params.storageClientHelper.listStorageRecords(params.serverURL, storageBundleId, pageRequest, params.authToken);
        storageContent = storageRecords.getResultList().stream()
                .flatMap(storageInfo -> params.storageClientHelper.listStorageContent(storageInfo.getConnectionURL(),
                        storageInfo.getId(),
                        params.authToken)
                        .stream()
                        .peek(contentInfo -> contentInfo.setRootLocation(storageInfo.getConnectionURL())))
                .filter(contentInfo -> !contentInfo.isCollectionFlag())
                .collect(Collectors.toList());
    }
}
