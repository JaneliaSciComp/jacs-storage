package org.janelia.jacsstorage.service.localservice;

import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.distributedservice.StorageAgentManager;

import javax.inject.Inject;

@LocalInstance
public class LocalStorageLookupService implements StorageLookupService {

    private final JacsStorageVolumeDao storageVolumeDao;
    private final JacsBundleDao bundleDao;

    @Inject
    public LocalStorageLookupService(JacsStorageVolumeDao storageVolumeDao,
                                     JacsBundleDao bundleDao) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
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
                .map(storageVolume -> {
                    bundle.setConnectionInfo(storageVolume.getMountHostIP());
                    bundle.setConnectionURL(storageVolume.getMountHostURL());
                    return bundle;
                });
    }

}
