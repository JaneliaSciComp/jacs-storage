package org.janelia.jacsstorage.service.localservice;

import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
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

@LocalInstance
public class LocalStorageLookupService implements StorageLookupService {

    private final ReadOnlyDao<JacsStorageVolume> storageVolumeDao;
    private final JacsBundleDao bundleDao;

    @Inject
    public LocalStorageLookupService(@Cacheable ReadOnlyDao<JacsStorageVolume> storageVolumeDao,
                                     JacsBundleDao bundleDao) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
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
    public JacsBundle findDataBundleByOwnerKeyAndName(String owner, String name) {
        JacsBundle bundle = bundleDao.findByOwnerKeyAndName(owner, name);
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
        return bundleDao.findMatchingDataBundles(pattern, pageRequest);
    }

    private void updateStorageVolume(JacsBundle bundle) {
        JacsStorageVolume sv = storageVolumeDao.findById(bundle.getStorageVolumeId());
        if (sv != null) {
            bundle.setStorageVolume(sv);
        } else {
            throw new IllegalStateException("Invalid storage volume associated with " + bundle.getId());
        }
    }

}
