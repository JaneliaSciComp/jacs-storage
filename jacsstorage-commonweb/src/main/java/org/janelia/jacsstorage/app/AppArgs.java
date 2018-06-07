package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;

import java.util.Map;

public class AppArgs {
    @Parameter(names = "-b", description = "Binding IP", required = false)
    public String host = "localhost";
    @Parameter(names = "-p", description = "Listener port number", required = false)
    public int portNumber = 8080;
    @Parameter(names = "-name", description = "Deployment name", required = false)
    public String deployment = "jacsstorage";
    @Parameter(names = "-context-path", description = "Base context path", required = false)
    public String baseContextPath = "/jacsstorage";
    @Parameter(names = "-nio", description = "Number of IO threads", required = false)
    public int nIOThreads = 64;
    @Parameter(names = "-nworkers", description = "Number of worker threads", required = false)
    public int nWorkers = 64 * 8;
    @Parameter(names = "-appId", description = "application ID")
    public String applicationId;
    @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
    protected boolean displayUsage = false;
    @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
    private Map<String, String> applicationArgs = ApplicationConfigProvider.applicationArgs();
}
