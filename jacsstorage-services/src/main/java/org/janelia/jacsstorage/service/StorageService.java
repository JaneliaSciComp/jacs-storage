package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageDao;
import org.janelia.jacsstorage.dao.JacsVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

import javax.inject.Inject;

public class StorageService {

    private final JacsStorageDao storageDao;
    private final JacsVolumeDao volumeDao;
    private final JacsBundleDao bundleDao;

    @Inject
    public StorageService(JacsStorageDao storageDao, JacsVolumeDao volumeDao, JacsBundleDao bundleDao) {
        this.storageDao = storageDao;
        this.volumeDao = volumeDao;
        this.bundleDao = bundleDao;
    }

    public JacsBundle getData(String owner, String name) {
        JacsBundle bundle = bundleDao.findByNameAndOwner(owner, name);
        if (bundle != null) {
            bundle.setVolume(volumeDao.findById(bundle.getVolumeId()))
                    .flatMap(jacsVolume -> bundle.setStorage(storageDao.findById(jacsVolume.getStorageId())));
        }
        return bundle;
    }

}
