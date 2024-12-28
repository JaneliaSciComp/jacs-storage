package org.janelia.jacsstorage.agent.cmd;

import com.beust.jcommander.Parameter;

import org.janelia.jacsstorage.service.cmd.BenchmarksCmdLineParams;

public class AgentBenchmarksCmdLineParams extends BenchmarksCmdLineParams {
    @Parameter(names = "--vol-id", description = "Storage volume ID")
    String storageVolumeId = "";

    @Parameter(names = "--agent-url", description = "Storage agent URL")
    String storageAgentURL = "";
}
