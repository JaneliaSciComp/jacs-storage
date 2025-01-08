package org.janelia.jacsstorage.agent.benchmarks.cmd;

import com.beust.jcommander.Parameter;

import org.janelia.jacsstorage.service.benchmarks.cmd.BenchmarksCmdLineParams;

public class AgentBenchmarksCmdLineParams extends BenchmarksCmdLineParams {
    @Parameter(names = "--vol-id", description = "Storage volume ID")
    String storageVolumeId = "";

    @Parameter(names = "--agent-url", description = "Storage agent URL")
    String storageAgentURL = "";
}
