package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@State(Scope.Thread)
public class StreamContentBenchmarkInvocationParams {
    Number storageBundleId;
    List<DataNodeInfo> storageContent;
    Map<String, DataStorageInfo> storageInfoMap;

    @Setup(Level.Invocation)
    public void setUp(RetrieveBenchmarkTrialParams params) {
        storageBundleId = params.dataBundleId;
        PageRequestBuilder pageRequestBuilder = new PageRequestBuilder().pageSize(1);
        if (params.nStorageRecords > 0) {
            pageRequestBuilder.pageNumber(RandomUtils.nextLong(0, params.nStorageRecords));
        }
        if (storageBundleId == null || storageBundleId.toString().equals("0") || storageContent == null) {
            storageInfoMap = new HashMap<>();
            PageRequest pageRequest = pageRequestBuilder.build();
            PageResult<DataStorageInfo> storageRecords = params.storageClientHelper.listStorageRecords(params.serverURL, params.storageHost, params.getStorageTags(), storageBundleId, pageRequest, params.authToken);
            storageContent = storageRecords.getResultList().stream()
                    .flatMap(storageInfo -> params.storageClientHelper.listStorageContent(storageInfo.getConnectionURL(),
                            storageInfo.getNumericId(),
                            params.authToken)
                            .stream()
                            .peek((DataNodeInfo contentInfo) -> {
                                storageInfoMap.put(contentInfo.getStorageId(), storageInfo);
                                contentInfo.setRootLocation(storageInfo.getConnectionURL());
                            }))
                    .filter(contentInfo -> !contentInfo.isCollectionFlag())
                    .filter(contentInfo -> StringUtils.isBlank(params.storageEntry) || contentInfo.getNodeRelativePath().equals(params.storageEntry))
                    .collect(Collectors.toList());
        }
    }
}
