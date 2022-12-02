package org.janelia.jacsstorage.benchmarks;

import com.google.common.base.Splitter;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.client.clientutils.StorageClientImplHelper;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.Collections;
import java.util.List;

@State(Scope.Benchmark)
public class BenchmarkTrialParams {
    StorageClientImplHelper storageClientHelper;
    @Param({""})
    String serverURL;
    @Param({""})
    String agentURL;
    @Param({""})
    String ownerKey;
    @Param({""})
    String dataLocation;
    @Param({""})
    String dataFormat;
    @Param({""})
    String authToken;
    @Param({"0"})
    Long dataBundleId;
    @Param({""})
    String storageContext;
    @Param({""})
    String storageAgentId;
    @Param({""})
    private String storageTags;
    @Param({""})
    String updatedDataPath;

    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        serverURL = params.getParam("serverURL");
        agentURL = params.getParam("agentURL");
        ownerKey = params.getParam("ownerKey");
        dataLocation = params.getParam("dataLocation");
        dataFormat = params.getParam("dataFormat");
        authToken = params.getParam("authToken");
        String dataBundleIdParam = params.getParam("dataBundleId");
        if (StringUtils.isNotBlank(dataBundleIdParam)) {
            dataBundleId = Long.valueOf(dataBundleIdParam);
        }
        String storageContextParam = params.getParam("storageContext");
        if (StringUtils.isNotBlank(storageContextParam)) {
            storageContext = storageContextParam;
        }
        String storageAgentIdParam = params.getParam("storageAgentId");
        if (StringUtils.isNotBlank(storageAgentIdParam)) {
            storageAgentId = storageAgentIdParam;
        }
        String storageTagsParam = params.getParam("storageTags");
        if (StringUtils.isNotBlank(storageTagsParam)) {
            storageTags = storageTagsParam;
        }
        updatedDataPath = params.getParam("updatedDataPath");
        storageClientHelper = new StorageClientImplHelper("Benchmarks");
    }

    List<String> getStorageTags() {
        if (StringUtils.isNotBlank(storageTags)) {
            return Splitter.on(",").omitEmptyStrings().splitToList(storageTags);
        } else {
            return Collections.emptyList();
        }
    }
}
