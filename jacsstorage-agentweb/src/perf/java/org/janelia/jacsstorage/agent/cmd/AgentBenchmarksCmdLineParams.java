package org.janelia.jacsstorage.agent.cmd;

import com.beust.jcommander.Parameter;
import org.janelia.jacsstorage.service.cmd.BenchmarksCmdLineParams;

public class AgentBenchmarksCmdLineParams extends BenchmarksCmdLineParams {
    @Parameter(names = "-volId", description = "Storage volume ID")
    String storageVolumeId;
}
