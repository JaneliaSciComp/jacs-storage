package org.janelia.jacsstorage.service.distributedservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

public class RandomLocalStorageVolumeSelector implements StorageVolumeSelector {
    private static final Logger LOG = LoggerFactory.getLogger(RandomLocalStorageVolumeSelector.class);
    private static final Random RANDOM_SELECTOR = new Random(System.currentTimeMillis());

    private final JacsStorageVolumeDao storageVolumeDao;
    private final List<String> availableHosts;
    private final List<String> availableServicesURLs;

    RandomLocalStorageVolumeSelector(JacsStorageVolumeDao storageVolumeDao, List<String> availableHosts, List<String> availableServicesURLs) {
        this.storageVolumeDao = storageVolumeDao;
        this.availableHosts = availableHosts;
        this.availableServicesURLs = availableServicesURLs;
    }

    @Override
    public JacsStorageVolume selectStorageVolume(JacsBundle storageRequest) {
        StorageQuery storageQuery = new StorageQuery()
                .setStorageHosts(availableHosts)
                .setLocalToAnyHost(true)
                .setStorageAgents(availableServicesURLs);
        storageRequest.getStorageVolume()
                .ifPresent(sv -> {
                    storageQuery.setId(sv.getId());
                    storageQuery.setStorageName(sv.getName());
                    storageQuery.setStorageVirtualPath(sv.getStorageVirtualPath());
                    storageQuery.setStorageTags(sv.getStorageTags());
                    storageQuery.setAccessibleOnHost(sv.getStorageHost());
                });
        if (storageRequest.hasUsedSpaceSet()) {
            storageQuery.setMinAvailableSpaceInBytes(storageRequest.getUsedSpaceInBytes());
        }
        long storageVolumeResultsCount = storageVolumeDao.countMatchingVolumes(storageQuery);
        if (storageVolumeResultsCount == 0) {
            LOG.info("Found no volumes using query {}", storageQuery);
            return null;
        } else {
            int randomVolumeIndex = RANDOM_SELECTOR.nextInt((int) storageVolumeResultsCount);
            LOG.info("Found {} volumes using query {} - will try to get volume at {}",
                    storageVolumeResultsCount,
                    storageQuery,
                    randomVolumeIndex);
            PageRequest pageRequest = new PageRequest();
            pageRequest.setFirstPageOffset(randomVolumeIndex);
            pageRequest.setPageSize(1);
            PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest);
            if (storageVolumeResults.getResultList().isEmpty()) {
                return null;
            } else {
                return storageVolumeResults.getResultList().get(0);
            }
        }
    }

}
