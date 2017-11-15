package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.AbstractStorageAllocatorService;
import org.janelia.jacsstorage.service.OverflowStorageVolumeSelector;
import org.janelia.jacsstorage.service.RandomStorageVolumeSelector;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Optional;

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
        checkStorageAccess(credentials, existingBundle);
        return existingBundle.setStorageVolume(storageVolumeDao.findById(existingBundle.getStorageVolumeId()))
                .flatMap(sv -> agentManager.findRegisteredAgent(sv.getStorageServiceURL()))
                .map(storageAgentInfo -> {
                    if (AgentConnectionHelper.deleteStorage(storageAgentInfo.getAgentHttpURL(), existingBundle.getId(), credentials.getAuthToken())) {
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
        StorageVolumeSelector[] volumeSelectors = new StorageVolumeSelector[] {
                new RandomStorageVolumeSelector(storageVolumeDao),
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
            return agentManager.findRandomRegisteredAgent((StorageAgentConnection ac) -> ac.isConnected())
                    .map((StorageAgentInfo ai) -> {
                        selectedVolume.setStorageServiceURL(ai.getAgentHttpURL());
                        selectedVolume.setStorageServiceTCPPortNo(ai.getTcpPortNo());
                        return selectedVolume;
                    });
        } else {
            return Optional.of(selectedVolume);
        }
    }
}
