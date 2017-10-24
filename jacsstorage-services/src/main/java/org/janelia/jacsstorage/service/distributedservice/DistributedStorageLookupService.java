package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.inject.Inject;

@RemoteInstance
public class DistributedStorageLookupService implements StorageLookupService {

    private final JacsStorageVolumeDao storageVolumeDao;
    private final JacsBundleDao bundleDao;
    private final StorageAgentManager agentManager;

    @Inject
    public DistributedStorageLookupService(JacsStorageVolumeDao storageVolumeDao,
                                           JacsBundleDao bundleDao,
                                           StorageAgentManager agentManager) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
        this.agentManager = agentManager;
    }

    @Override
    public JacsBundle findDataBundleByOwnerAndName(String owner, String name) {
        JacsBundle bundle = bundleDao.findByOwnerAndName(owner, name);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        JacsBundle matchingPattern = pattern.getStorageVolume()
                .map(JacsStorageVolume::getLocation)
                .flatMap(storageVolumeDao::findStorageByLocation)
                .map(storageVolume -> {
                    pattern.setStorageVolumeId(storageVolume.getId());
                    return pattern;
                })
                .orElse(pattern)
                ;
        return bundleDao.findMatchingDataBundles(matchingPattern, pageRequest);
    }

    @Override
    public JacsBundle getDataBundleById(Number id) {
        JacsBundle bundle = bundleDao.findById(id);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    private void updateStorageVolume(JacsBundle bundle) {
        bundle.setStorageVolume(storageVolumeDao.findById(bundle.getStorageVolumeId()))
                .flatMap(storageVolume -> agentManager.findRegisteredAgentByLocationOrConnectionInfo(storageVolume.getLocation())) // find a registered agent that serves the given location
                .map(storageAgent -> {
                    bundle.setConnectionInfo(storageAgent.getConnectionInfo());
                    bundle.setConnectionURL(storageAgent.getAgentURL());
                    return bundle;
                });
    }

}
