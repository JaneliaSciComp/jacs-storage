package org.janelia.jacsstorage.service.impl.distributedservice;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

class DistributedStorageHelper {
    private final StorageAgentManager agentManager;

    DistributedStorageHelper(StorageAgentManager agentManager) {
        this.agentManager = agentManager;
    }

    void fillStorageAccessInfo(JacsStorageVolume storageVolume) {
        if (storageVolume != null) {
            agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected() && ac.getAgentInfo().canServe(storageVolume))
                    .map(ai -> {
                        storageVolume.setStorageAgentId(ai.getAgentId());
                        storageVolume.setStorageServiceURL(ai.getAgentAccessURL());
                        return true;
                    })
                    .orElseGet(() -> {
                        // reset access info because the storage is actually not accessible
                        storageVolume.setStorageServiceURL(null);
                        return false;
                    })
                    ;
        }
    }

    boolean isAccessible(JacsStorageVolume storageVolume) {
        return storageVolume != null && !agentManager.getCurrentRegisteredAgents((StorageAgentConnection ac) -> ac.isConnected() && ac.getAgentInfo().canServe(storageVolume)).isEmpty();
    }
}
