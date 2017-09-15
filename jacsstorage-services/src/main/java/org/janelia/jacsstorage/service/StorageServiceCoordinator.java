package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import javax.inject.Inject;
import java.util.Optional;

public class StorageServiceCoordinator implements StorageService {

    private final StorageAgentManager agentManager;
    private final JacsStorageVolumeDao storageVolumeDao;
    private final JacsBundleDao bundleDao;

    @Inject
    public StorageServiceCoordinator(StorageAgentManager agentManager, JacsStorageVolumeDao storageVolumeDao, JacsBundleDao bundleDao) {
        this.agentManager = agentManager;
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
    }

    public Optional<JacsBundle> allocateStorage(JacsBundle dataBundle) {
        return agentManager.findRandomRegisteredAgent()
                .map(storageAgentInfo -> {
                    JacsStorageVolume storageVolume = storageVolumeDao.findOrCreateByLocation(storageAgentInfo.getLocation());
                    dataBundle.setStorageVolumeId(storageVolume.getId());
                    dataBundle.setStorageVolume(storageVolume);
                    bundleDao.save(dataBundle);
                    return dataBundle;
                });
    }

    public JacsBundle getDataBundleById(Number id) {
        JacsBundle bundle = bundleDao.findById(id);
        if (bundle != null) {
            bundle.setStorageVolume(storageVolumeDao.findById(bundle.getStorageVolumeId()));
        }
        return bundle;
    }

    public JacsBundle findDataBundleByOwnerAndName(String owner, String name) {
        JacsBundle bundle = bundleDao.findByOwnerAndName(owner, name);
        if (bundle != null) {
            bundle.setStorageVolume(storageVolumeDao.findById(bundle.getStorageVolumeId()));
        }
        return bundle;
    }

}
