package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.Random;

public class OverflowStorageVolumeSelector implements StorageVolumeSelector {
    private final JacsStorageVolumeDao storageVolumeDao;

    public OverflowStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao) {
        this.storageVolumeDao = storageVolumeDao;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        JacsStorageVolume volumePattern = new JacsStorageVolume();
        volumePattern.setStorageHost(null); // require a storageHost not to be set
        volumePattern.setShared(true);
        volumePattern.setStorageTags(storageRequest.getStorageTags());
        volumePattern.setName(JacsStorageVolume.OVERFLOW_VOLUME);
        PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(volumePattern, new PageRequest());
        if (storageVolumeResults.getResultList().isEmpty()) {
            return null;
        } else {
            return storageVolumeResults.getResultList().get(0);
        }
    }

}
