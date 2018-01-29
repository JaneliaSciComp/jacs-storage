package org.janelia.jacsstorage.service.distributedservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

class DistributedStorageHelper {
    private final StorageAgentManager agentManager;

    DistributedStorageHelper(StorageAgentManager agentManager) {
        this.agentManager = agentManager;
    }

    void updateStorageServiceInfo(JacsStorageVolume storageVolume) {
        if (storageVolume.isShared() && StringUtils.isBlank(storageVolume.getStorageHost())) {
            agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected())
                    .ifPresent(ai -> {
                        storageVolume.setStorageHost(ai.getStorageHost());
                        storageVolume.setStorageServiceURL(ai.getAgentHttpURL());
                    });
        }
    }
}
