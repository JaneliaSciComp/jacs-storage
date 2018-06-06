package org.janelia.jacsstorage.app;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;

import java.util.Map;

class AppArgs {
    @Parameter(names = "-b", description = "Binding IP", required = false)
    protected String host = "localhost";
    @Parameter(names = "-p", description = "Listener port number", required = false)
    protected int portNumber = 8080;
    @Parameter(names = "-name", description = "Deployment name", required = false)
    protected String deployment = "jacsstorage";
    @Parameter(names = "-context-path", description = "Base context path", required = false)
    protected String baseContextPath = "/jacsstorage";
    @Parameter(names = "-nio", description = "Number of IO threads", required = false)
    int nIOThreads = 64;
    @Parameter(names = "-nworkers", description = "Number of worker threads", required = false)
    int nWorkers = 64 * 8;
    @Parameter(names = "-appId", description = "application ID")
    protected String applicationId;
    @Parameter(names = "-h", description = "Display help", arity = 0, required = false)
    protected boolean displayUsage = false;
    @DynamicParameter(names = "-D", description = "Dynamic application parameters that could override application properties")
    private Map<String, String> applicationArgs = ApplicationConfigProvider.applicationArgs();
}
