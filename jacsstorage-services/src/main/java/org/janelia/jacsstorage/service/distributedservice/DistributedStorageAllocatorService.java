package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.impl.AbstractStorageAllocatorService;
import org.janelia.jacsstorage.service.impl.OverflowStorageVolumeSelector;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@RemoteInstance
public class DistributedStorageAllocatorService extends AbstractStorageAllocatorService {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedStorageAllocatorService.class);

    private final JacsStorageVolumeDao storageVolumeDao;
    private final StorageAgentManager agentManager;

    @Inject
    public DistributedStorageAllocatorService(JacsStorageVolumeDao storageVolumeDao,
                                              JacsBundleDao bundleDao,
                                              StorageAgentManager agentManager) {
        super(bundleDao);
        this.storageVolumeDao = storageVolumeDao;
        this.agentManager = agentManager;
    }

    @Override
    public boolean deleteStorage(JacsCredentials credentials, JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            return false;
        }
        checkStorageDeletePermission(credentials, existingBundle);
        return existingBundle.setStorageVolume(storageVolumeDao.findById(existingBundle.getStorageVolumeId()))
                .flatMap(sv -> agentManager.findRegisteredAgent(sv.getStorageServiceURL()))
                .map(storageAgentInfo -> {
                    if (AgentConnectionHelper.deleteStorage(storageAgentInfo.getAgentHttpURL(), existingBundle.getId(), credentials.getSubjectProxy(), credentials.getAuthToken())) {
                        LOG.info("Delete {}", existingBundle);
                        bundleDao.delete(existingBundle);
                        return true;
                    } else {
                        return false;
                    }
                })
                .orElse(false);
    }

    @Override
    public Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle) {
        List<StorageAgentInfo> availableAgents = agentManager.getCurrentRegisteredAgents(ac -> ac.isConnected());
        StorageVolumeSelector[] volumeSelectors = new StorageVolumeSelector[] {
                new RandomLocalStorageVolumeSelector(storageVolumeDao,
                        availableAgents.stream().map(ai -> ai.getStorageHost()).collect(Collectors.toList()),
                        availableAgents.stream().map(ai -> ai.getAgentHttpURL()).collect(Collectors.toList())),
                new RandomSharedStorageVolumeSelector(storageVolumeDao,
                        availableAgents.stream().map(ai -> ai.getAgentHttpURL()).collect(Collectors.toList())),
                new OverflowStorageVolumeSelector(storageVolumeDao)
        };
        JacsStorageVolume storageVolume = null;
        for (StorageVolumeSelector volumeSelector : volumeSelectors) {
            storageVolume = volumeSelector.selectStorageVolume(dataBundle);
            if (storageVolume != null) {
                break;
            }
        }
        if (storageVolume == null) {
            LOG.warn("No storage volume selected for {}", dataBundle);
            return Optional.empty();
        }
        JacsStorageVolume selectedVolume = storageVolume;
        if (selectedVolume.getStorageServiceURL() == null) {
            // find any connected agent
            return agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected())
                    .map((StorageAgentInfo ai) -> {
                        selectedVolume.setStorageHost(ai.getStorageHost());
                        selectedVolume.setStorageServiceURL(ai.getAgentHttpURL());
                        return selectedVolume;
                    });
        } else {
            // check that the agent which serves the volume is still connected
            return agentManager.findRegisteredAgent(selectedVolume.getStorageServiceURL())
                    .filter(ai -> StorageAgentConnection.CONNECTED_STATUS_VALUE.equals(ai.getConnectionStatus()))
                    .map(ai -> selectedVolume);
        }
    }
}
