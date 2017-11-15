package org.janelia.jacsstorage.service;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.utils.NetUtils;

import java.util.List;
import java.util.Optional;

public class LocalStorageVolumeSelector implements StorageVolumeSelector {
    private final String storageHost;
    private final JacsStorageVolumeDao storageVolumeDao;

    public LocalStorageVolumeSelector(String storageHost, JacsStorageVolumeDao storageVolumeDao) {
        this.storageHost = storageHost;
        this.storageVolumeDao = storageVolumeDao;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        JacsStorageVolume volumePattern = new JacsStorageVolume();
        volumePattern.setStorageHost(getStorageHost());
        volumePattern.setStorageTags(storageRequest.getStorageTags());
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    volumePattern.setId(sv.getId());
                    volumePattern.setName(sv.getName());
                });
        if (storageRequest.hasUsedSpaceSet()) {
            volumePattern.setAvailableSpaceInBytes(storageRequest.getUsedSpaceInBytes());
        }
        PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(volumePattern, new PageRequest());
        if (storageVolumeResults.getResultList().isEmpty()) {
            return null;
        } else {
            return storageVolumeResults.getResultList().get(0);
        }
    }

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostIP());
    }

}
