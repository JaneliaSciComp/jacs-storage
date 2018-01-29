package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public class OverflowStorageVolumeSelector implements StorageVolumeSelector {
    private final JacsStorageVolumeDao storageVolumeDao;

    public OverflowStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao) {
        this.storageVolumeDao = storageVolumeDao;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        StorageQuery storageQuery = new StorageQuery()
                .setShared(true)
                .setStorageName(JacsStorageVolume.OVERFLOW_VOLUME);
        PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(storageQuery, new PageRequest());
        if (storageVolumeResults.getResultList().isEmpty()) {
            return null;
        } else {
            return storageVolumeResults.getResultList().get(0);
        }
    }

}
