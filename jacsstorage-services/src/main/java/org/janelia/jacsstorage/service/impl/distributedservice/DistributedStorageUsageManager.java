package org.janelia.jacsstorage.service.impl.distributedservice;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;

@RemoteInstance
public class DistributedStorageUsageManager implements StorageUsageManager {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedStorageUsageManager.class);

    private final StorageVolumeManager storageVolumeManager;

    @Inject
    public DistributedStorageUsageManager(@RemoteInstance StorageVolumeManager storageVolumeManager) {
        this.storageVolumeManager = storageVolumeManager;
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByVolumeId(Number storageVolumeId) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setId(storageVolumeId));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", storageVolumeId);
            throw new IllegalArgumentException("No volume found for " + storageVolumeId);
        }
        return AgentConnectionHelper.retrieveVolumeUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storageVolumeId,
                null);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByVolumeIdForUser(Number storageVolumeId, String username) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setId(storageVolumeId));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", storageVolumeId);
            throw new IllegalArgumentException("No volume found for " + storageVolumeId);
        }
        List<UsageData> usageDataReport = AgentConnectionHelper.retrieveVolumeUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storageVolumeId,
                username);
        return CollectionUtils.isEmpty(usageDataReport) ? UsageData.EMPTY : usageDataReport.get(0);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByVolumeName(String volumeName) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setStorageName(volumeName));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        }
        return AgentConnectionHelper.retrieveVolumeUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storageVolumes.get(0).getId(),
                null);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByVolumeNameForUser(String volumeName, String username) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setStorageName(volumeName));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        }
        List<UsageData> usageDataReport = AgentConnectionHelper.retrieveVolumeUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storageVolumes.get(0).getId(),
                username);
        return CollectionUtils.isEmpty(usageDataReport) ? UsageData.EMPTY : usageDataReport.get(0);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByStoragePath(String storagePath) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setDataStoragePath(storagePath));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        }
        return AgentConnectionHelper.retrieveDataPathUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storagePath,
                null);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByStoragePathForUser(String storagePath, String username) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setDataStoragePath(storagePath));
        if (CollectionUtils.isEmpty(storageVolumes)) {
            LOG.warn("No volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        }
        List<UsageData> usageDataReport = AgentConnectionHelper.retrieveDataPathUsageData(storageVolumes.get(0).getStorageServiceURL(),
                storagePath,
                username);
        return CollectionUtils.isEmpty(usageDataReport) ? UsageData.EMPTY : usageDataReport.get(0);
    }
}
