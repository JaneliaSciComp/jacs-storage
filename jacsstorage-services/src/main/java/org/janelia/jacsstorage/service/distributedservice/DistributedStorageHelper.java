package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

class DistributedStorageHelper {
    private final StorageAgentManager agentManager;

    DistributedStorageHelper(StorageAgentManager agentManager) {
        this.agentManager = agentManager;
    }

    void fillStorageAccessInfo(JacsStorageVolume storageVolume) {
        if (storageVolume.isShared()) {
            agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected() && ac.getAgentInfo().canServe(storageVolume))
                    .ifPresent(ai -> {
                        storageVolume.setStorageAgentId(ai.getAgentId());
                        storageVolume.setStorageServiceURL(ai.getAgentAccessURL());
                    });
        }
    }
}
