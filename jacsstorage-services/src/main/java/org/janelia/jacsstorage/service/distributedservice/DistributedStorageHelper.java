package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

class DistributedStorageHelper {
    private final StorageAgentManager agentManager;

    DistributedStorageHelper(StorageAgentManager agentManager) {
        this.agentManager = agentManager;
    }

    void updateStorageServiceInfo(JacsStorageVolume storageVolume) {
        if (storageVolume.isShared()) {
            agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected() && ac.getAgentInfo().canServe(storageVolume))
                    .ifPresent(ai -> {
                        storageVolume.setStorageHost(ai.getAgentHost());
                        storageVolume.setStorageServiceURL(ai.getAgentAccessURL());
                    });
        }
    }
}
