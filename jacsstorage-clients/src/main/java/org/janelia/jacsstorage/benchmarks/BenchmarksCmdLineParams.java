package org.janelia.jacsstorage.benchmarks;

import com.beust.jcommander.Parameter;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.util.ArrayList;
import java.util.List;

class BenchmarksCmdLineParams {
    @Parameter(names = "-warmup", description = "Warmup iterations")
    int warmupIterations = 5;
    @Parameter(names = "-measurements", description = "Measurement iterations")
    int measurementIterations = 5;
    @Parameter(names = "-forks", description = "Number of process instances")
    int nForks = 1;
    @Parameter(names = "-threads", description = "Number of threads")
    int nThreads = 5;
    @Parameter(names = "-server", description = "Master storage server URL")
    String serverURL = "http://jdcu1:8880/jacsstorage/master_api/v1";
    @Parameter(names = "-dataFormat", description = "Data bundle format")
    JacsStorageFormat dataFormat = JacsStorageFormat.DATA_DIRECTORY;
    @Parameter(names = "-storageHost", description = "Storage tags")
    String storageHost = "";
    @Parameter(names = "-storageTags", description = "Storage tags")
    List<String> storageTags = new ArrayList<>();
    @Parameter(names = "-storageContext", description = "Storage path context")
    String storageContext = "";
    @Parameter(names = "-localPath", description = "Local path")
    String localPath = "";
    @Parameter(names = "-entryName", description = "Entry name")
    String entryName = "";
    // authentication params
    @Parameter(names = "-authServer", description = "Authentication server URL")
    String authURL = "https://jacs-dev.int.janelia.org//SCSW/AuthenticationService/v1";
    @Parameter(names = "-username", description = "User name")
    String username;
    @Parameter(names = "-password", description = "User password")
    String password;
    // storage update params
    @Parameter(names = "-bundleId", description = "bundle id")
    Long bundleId = 0L;
    @Parameter(names = "-updatedPath", description = "updated bundle path")
    String updatedPath = "";
    @Parameter(names = "-benchmarksRegex", description = "benchmarks to be run regex")
    String benchmarksRegex;

    String getUserKey() {
        return "user:" + username;
    }

    String getStorageTagsAsString() {
        return storageTags.stream().reduce((t1, t2) -> t1 + "," + t2).orElse("");
    }
}