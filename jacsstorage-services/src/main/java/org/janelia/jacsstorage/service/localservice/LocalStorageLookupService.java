package org.janelia.jacsstorage.service.localservice;

import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageEventLogger;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.inject.Inject;

@LocalInstance
public class LocalStorageLookupService implements StorageLookupService {

    private final JacsBundleDao bundleDao;

    @Inject
    public LocalStorageLookupService(JacsBundleDao bundleDao) {
        this.bundleDao = bundleDao;
    }

    @Override
    public JacsBundle findDataBundleByOwnerAndName(String owner, String name) {
        return bundleDao.findByOwnerAndName(owner, name);
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        return bundleDao.findMatchingDataBundles(pattern, pageRequest);
    }

    @Override
    public JacsBundle getDataBundleById(Number id) {
        return bundleDao.findById(id);
    }

}
