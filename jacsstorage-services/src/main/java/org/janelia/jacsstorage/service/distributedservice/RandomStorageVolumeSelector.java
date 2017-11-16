package org.janelia.jacsstorage.service.distributedservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.janelia.jacsstorage.utils.NetUtils;

import java.util.List;
import java.util.Random;

public class RandomStorageVolumeSelector implements StorageVolumeSelector {
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final JacsStorageVolumeDao storageVolumeDao;
    private final List<String> availableHosts;
    private final List<String> availableServicesURLs;

    public RandomStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao, List<String> availableHosts, List<String> availableServicesURLs) {
        this.storageVolumeDao = storageVolumeDao;
        this.availableHosts = availableHosts;
        this.availableServicesURLs = availableServicesURLs;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        StorageQuery storageQuery = new StorageQuery()
                .setStorageHosts(availableHosts)
                .setLocalToAnyHost(true)
                .setShared(false)
                .setStorageAgents(availableServicesURLs)
                .setStorageTags(storageRequest.getStorageTags());
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    storageQuery.setId(sv.getId());
                    storageQuery.setStorageName(sv.getName());
                });
        if (storageRequest.hasUsedSpaceSet()) {
            storageQuery.setMinAvailableSpaceInBytes(storageRequest.getUsedSpaceInBytes());
        }
        long storageVolumeResultsCount = storageVolumeDao.countMatchingVolumes(storageQuery);
        if (storageVolumeResultsCount == 0) {
            return null;
        } else {
            PageRequest pageRequest = new PageRequest();
            pageRequest.setFirstPageOffset(RANDOM_SELECTOR.nextInt((int) storageVolumeResultsCount));
            PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest);
            if (storageVolumeResults.getResultList().isEmpty()) {
                return null;
            } else {
                return storageVolumeResults.getResultList().get(0);
            }
        }
    }

}
