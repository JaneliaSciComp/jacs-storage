package org.janelia.jacsstorage.service.localservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.janelia.jacsstorage.utils.NetUtils;

public class LocalStorageVolumeSelector implements StorageVolumeSelector {
    private final JacsStorageVolumeDao storageVolumeDao;
    private final String storageHost;

    public LocalStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao, String storageHost) {
        this.storageVolumeDao = storageVolumeDao;
        this.storageHost = storageHost;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        StorageQuery storageQuery = new StorageQuery()
                .addStorageHost(getStorageHost())
                .setLocalToAnyHost(true);
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    storageQuery.setId(sv.getId());
                    storageQuery.setStorageName(sv.getName());
                    storageQuery.setStoragePathPrefix(sv.getStoragePathPrefix());
                    storageQuery.setStorageTags(sv.getStorageTags());
                });
        if (storageRequest.hasUsedSpaceSet()) {
            storageQuery.setMinAvailableSpaceInBytes(storageRequest.getUsedSpaceInBytes());
        }
        PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(storageQuery, new PageRequest());
        if (storageVolumeResults.getResultList().isEmpty()) {
            return null;
        } else {
            return storageVolumeResults.getResultList().get(0);
        }
    }

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

}
