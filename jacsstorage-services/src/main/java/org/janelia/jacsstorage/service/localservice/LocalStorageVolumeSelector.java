package org.janelia.jacsstorage.service.localservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.janelia.jacsstorage.coreutils.NetUtils;

public class LocalStorageVolumeSelector implements StorageVolumeSelector {
    private final JacsStorageVolumeDao storageVolumeDao;
    private final String storageAgentId;

    public LocalStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao, String storageAgentId) {
        this.storageVolumeDao = storageVolumeDao;
        this.storageAgentId = storageAgentId;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        StorageQuery storageQuery = new StorageQuery()
                .addStorageAgent(storageAgentId)
                .setLocalToAnyAgent(true);
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    storageQuery.setId(sv.getId());
                    storageQuery.setStorageName(sv.getName());
                    storageQuery.setStorageVirtualPath(sv.getStorageVirtualPath());
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

}
