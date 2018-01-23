package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.CacheableEntityByIdDao;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.dao.ReadOnlyDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.inject.Inject;

@RemoteInstance
public class DistributedStorageLookupService implements StorageLookupService {

    private final ReadOnlyDao<JacsStorageVolume> storageVolumeDao;
    private final JacsBundleDao bundleDao;
    private final DistributedStorageHelper storageHelper;

    @Inject
    public DistributedStorageLookupService(@Cacheable ReadOnlyDao<JacsStorageVolume> storageVolumeDao,
                                           JacsBundleDao bundleDao,
                                           StorageAgentManager agentManager) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
        this.storageHelper = new DistributedStorageHelper(agentManager);
    }

    @Override
    public JacsBundle getDataBundleById(Number id) {
        JacsBundle bundle = bundleDao.findById(id);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    @Override
    public JacsBundle findDataBundleByOwnerKeyAndName(String ownerKey, String name) {
        JacsBundle bundle = bundleDao.findByOwnerKeyAndName(ownerKey, name);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    @Override
    public long countMatchingDataBundles(JacsBundle pattern) {
        return bundleDao.countMatchingDataBundles(pattern);
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        PageResult<JacsBundle> matchingBundles = bundleDao.findMatchingDataBundles(pattern, pageRequest);
        matchingBundles.getResultList().stream()
                .filter(db -> !db.hasStorageHost())
                .forEach(this::updateStorageVolume)
        ;
        return matchingBundles;
    }

    private void updateStorageVolume(JacsBundle bundle) {
        JacsStorageVolume bundleVol = bundle.getStorageVolume()
                .orElseGet(() -> {
                    JacsStorageVolume sv = storageVolumeDao.findById(bundle.getStorageVolumeId());
                    bundle.setStorageVolume(sv);
                    return sv;
                });
        storageHelper.updateStorageServiceInfo(bundleVol);
    }
}
