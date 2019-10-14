package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;

import java.util.HashMap;
import java.util.Map;

public class AppArgs {
    @Parameter(names = "-b", description = "Binding IP")
    public String host = "localhost";
    @Parameter(names = "-p", description = "Listener port number")
    public int portNumber = 8080;
    @Parameter(names = "-name", description = "Deployment name")
    public String deployment = "jacsstorage";
    @Parameter(names = "-context-path", description = "Base context path")
    public String baseContextPath = "/jacsstorage";
    @Parameter(names = "-nio", description = "Number of IO threads")
    public int nIOThreads = 64;
    @Parameter(names = "-nworkers", description = "Number of worker threads")
    public int nWorkers = 64 * 8;
    @Parameter(names = "-responseTimeout", description = "Server response write timeout in seconds")
    public int serverResponseTimeoutInSeconds = 120;
    @Parameter(names = "-appId", description = "application ID")
    String applicationId;
    @Parameter(names = "-h", description = "Display help", arity = 0)
    boolean displayUsage = false;
    @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
    Map<String, String> appDynamicConfig = new HashMap<>();
}
