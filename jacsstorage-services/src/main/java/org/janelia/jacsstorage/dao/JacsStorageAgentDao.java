package org.janelia.jacsstorage.dao;

import java.util.Set;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;

public interface JacsStorageAgentDao extends ReadWriteDao<JacsStorageAgent> {
    JacsStorageAgent createStorageAgentIfNotFound(String agentHost, String agentAccessURL, String status, Set<String> servedVolumes);
    JacsStorageAgent findStorageAgentByHost(String agentHost);
}
