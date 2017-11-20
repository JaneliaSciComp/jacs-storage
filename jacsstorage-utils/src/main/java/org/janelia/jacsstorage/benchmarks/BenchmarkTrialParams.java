package org.janelia.jacsstorage.benchmarks;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.utils.StorageClientImplHelper;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;

@State(Scope.Benchmark)
public class BenchmarkTrialParams {
    StorageClientImplHelper storageClientHelper;
    @Param({""})
    String serverURL;
    @Param({"false"})
    Boolean useHttp;
    @Param({""})
    String owner;
    @Param({""})
    String dataLocation;
    @Param({""})
    String dataFormat;
    @Param({""})
    String authToken;
    @Param({"0"})
    Long dataBundleId;
    @Param({""})
    String updatedDataPath;


    @Setup(Level.Trial)
    public void setUp(BenchmarkParams params) {
        serverURL = params.getParam("serverURL");
        useHttp = Boolean.valueOf(params.getParam("useHttp"));
        owner = params.getParam("owner");
        dataLocation = params.getParam("dataLocation");
        dataFormat = params.getParam("dataFormat");
        authToken = params.getParam("authToken");
        String dataBundleIdParam = params.getParam("dataBundleId");
        if (StringUtils.isNotBlank(dataBundleIdParam)) {
            dataBundleId = Long.valueOf(dataBundleIdParam);
        }
        updatedDataPath = params.getParam("updatedDataPath");
        storageClientHelper = new StorageClientImplHelper();
    }
}
