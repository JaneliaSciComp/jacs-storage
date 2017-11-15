package org.janelia.jacsstorage.service;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.utils.NetUtils;

import java.util.Random;

public class RandomStorageVolumeSelector implements StorageVolumeSelector {
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final JacsStorageVolumeDao storageVolumeDao;

    public RandomStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao) {
        this.storageVolumeDao = storageVolumeDao;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        JacsStorageVolume volumePattern = new JacsStorageVolume();
        volumePattern.setStorageHost(""); // require a storageHost to be set
        volumePattern.setStorageTags(storageRequest.getStorageTags());
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    volumePattern.setId(sv.getId());
                    volumePattern.setName(sv.getName());
                });
        if (storageRequest.hasUsedSpaceSet()) {
            volumePattern.setAvailableSpaceInBytes(storageRequest.getUsedSpaceInBytes());
        }
        long storageVolumeResultsCount = storageVolumeDao.countMatchingVolumes(volumePattern);
        if (storageVolumeResultsCount == 0) {
            return null;
        } else {
            PageRequest pageRequest = new PageRequest();
            pageRequest.setFirstPageOffset(RANDOM_SELECTOR.nextInt((int) storageVolumeResultsCount));
            PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(volumePattern, pageRequest);
            if (storageVolumeResults.getResultList().isEmpty()) {
                return null;
            } else {
                return storageVolumeResults.getResultList().get(0);
            }
        }
    }

}
