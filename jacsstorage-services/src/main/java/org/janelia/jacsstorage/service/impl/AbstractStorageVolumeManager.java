package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStorageVolumeManager implements StorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractStorageVolumeManager.class);
    private static final Integer FILL_UP_THRESHOLD = 85;

    protected final JacsStorageVolumeDao storageVolumeDao;
    private final NotificationService capacityNotifier;

    protected AbstractStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao, NotificationService capacityNotifier) {
        this.storageVolumeDao = storageVolumeDao;
        this.capacityNotifier = capacityNotifier;
    }

    @TimedMethod
    @Override
    public JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume) {
        storageVolumeDao.save(storageVolume);
        return storageVolume;
    }

    @TimedMethod
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageAgentId) {
        return storageVolumeDao.createStorageVolumeIfNotFound(volumeName, storageAgentId);
    }

    @TimedMethod(
            logLevel = "trace"
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        JacsStorageVolume currentVolumeInfo = storageVolumeDao.findById(volumeId);
        Preconditions.checkArgument(currentVolumeInfo != null, "Invalid storage volume ID: " + volumeId);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
        if (currentVolumeInfo.getStorageRootTemplate() == null ||
                (StringUtils.isNotBlank(storageVolume.getStorageRootTemplate()) &&
                        !storageVolume.getStorageRootTemplate().equals(currentVolumeInfo.getStorageRootTemplate()))) {
            currentVolumeInfo.setStorageRootTemplate(storageVolume.getStorageRootTemplate());
            updatedVolumeFieldsBuilder.put("storageRootTemplate", new SetFieldValueHandler<>(storageVolume.getStorageRootTemplate()));
        }
        if (currentVolumeInfo.getStorageVirtualPath() == null ||
                (StringUtils.isNotBlank(storageVolume.getStorageVirtualPath()) &&
                        !storageVolume.getStorageVirtualPath().equals(currentVolumeInfo.getStorageVirtualPath()))) {
            currentVolumeInfo.setStorageVirtualPath(storageVolume.getStorageVirtualPath());
            updatedVolumeFieldsBuilder.put("storageVirtualPath", new SetFieldValueHandler<>(storageVolume.getStorageVirtualPath()));
        }
        if (storageVolume.isNotShared()) {
            if (StringUtils.isNotBlank(storageVolume.getStorageServiceURL()) &&
                    (StringUtils.isBlank(currentVolumeInfo.getStorageServiceURL()) ||
                            !storageVolume.getStorageServiceURL().equals(currentVolumeInfo.getStorageServiceURL()))) {
                // if the storage access URLs differ
                currentVolumeInfo.setStorageServiceURL(storageVolume.getStorageServiceURL());
                updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(storageVolume.getStorageServiceURL()));
            }
        } else if (currentVolumeInfo.isNotShared()) {
            // if somehow the current volume is not shared make it shared
            currentVolumeInfo.setShared(true);
            currentVolumeInfo.setStorageServiceURL(null);
            currentVolumeInfo.setStorageAgentId(null);
            updatedVolumeFieldsBuilder.put("shared", new SetFieldValueHandler<>(true));
            updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(null));
            updatedVolumeFieldsBuilder.put("storageHost", new SetFieldValueHandler<>(null));
        }

        if (currentVolumeInfo.isActiveFlag()) {
            updatedVolumeFieldsBuilder.putAll(handleVolumeFillupStatus(currentVolumeInfo));
        }

        if (!currentVolumeInfo.hasTags() && storageVolume.hasTags() ||
                currentVolumeInfo.hasTags() && !storageVolume.hasTags() ||
                currentVolumeInfo.hasTags() && storageVolume.hasTags() && !ImmutableSet.copyOf(currentVolumeInfo.getStorageTags()).equals(ImmutableSet.copyOf(storageVolume.getStorageTags()))) {
            // if the tags differ
            currentVolumeInfo.setStorageTags(storageVolume.getStorageTags());
            updatedVolumeFieldsBuilder.put("storageTags", new SetFieldValueHandler<>(storageVolume.getStorageTags()));
        }
        if (!currentVolumeInfo.hasPermissions() && storageVolume.hasPermissions() ||
                currentVolumeInfo.hasPermissions() && !storageVolume.hasPermissions() ||
                currentVolumeInfo.hasPermissions() && storageVolume.hasPermissions() && !currentVolumeInfo.getVolumePermissions().equals(storageVolume.getVolumePermissions())) {
            // if the permissions differ
            currentVolumeInfo.setVolumePermissions(storageVolume.getVolumePermissions());
            updatedVolumeFieldsBuilder.put("volumePermissions", new SetFieldValueHandler<>(storageVolume.getVolumePermissions()));
        }
        if (storageVolume.getQuotaFailPercent() != null && !storageVolume.getQuotaFailPercent().equals(currentVolumeInfo.getQuotaFailPercent())) {
            currentVolumeInfo.setQuotaFailPercent(storageVolume.getQuotaFailPercent());
            updatedVolumeFieldsBuilder.put("quotaFailPercent", new SetFieldValueHandler<>(storageVolume.getQuotaFailPercent()));
        }
        if (storageVolume.getQuotaWarnPercent() != null && !storageVolume.getQuotaWarnPercent().equals(currentVolumeInfo.getQuotaWarnPercent())) {
            currentVolumeInfo.setQuotaWarnPercent(storageVolume.getQuotaWarnPercent());
            updatedVolumeFieldsBuilder.put("quotaWarnPercent", new SetFieldValueHandler<>(storageVolume.getQuotaWarnPercent()));
        }
        if (StringUtils.isNotBlank(storageVolume.getSystemUsageFile()) && !storageVolume.getSystemUsageFile().equals(currentVolumeInfo.getSystemUsageFile())) {
            currentVolumeInfo.setSystemUsageFile(storageVolume.getSystemUsageFile());
            updatedVolumeFieldsBuilder.put("systemUsageFile", new SetFieldValueHandler<>(storageVolume.getSystemUsageFile()));
        }
        if (currentVolumeInfo.isActiveFlag() != storageVolume.isActiveFlag()) {
            currentVolumeInfo.setActiveFlag(storageVolume.isActiveFlag());
            updatedVolumeFieldsBuilder.put("activeFlag", new SetFieldValueHandler<>(storageVolume.isActiveFlag()));
        }
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = updatedVolumeFieldsBuilder.build();
        if (updatedVolumeFields.isEmpty()) {
            return currentVolumeInfo;
        } else {
            return storageVolumeDao.update(currentVolumeInfo.getId(), updatedVolumeFieldsBuilder.build());
        }
    }

    private Map<String, EntityFieldValueHandler<?>> handleVolumeFillupStatus(JacsStorageVolume storageVolume) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();

        long availableSpaceInBytes = calculateAvailableStorageSpaceInBytes(storageVolume.getBaseStorageRootDir());
        if (!Long.valueOf(availableSpaceInBytes).equals(storageVolume.getAvailableSpaceInBytes())) {
            LOG.trace("Update availableSpace for volume {}:{} to {} bytes", storageVolume.getId(), storageVolume.getName(), availableSpaceInBytes);
            storageVolume.setAvailableSpaceInBytes(availableSpaceInBytes);
            updatedVolumeFieldsBuilder.put("availableSpaceInBytes", new SetFieldValueHandler<>(availableSpaceInBytes));
        }

        long totalSpaceInBytes = calculateTotalSpaceInBytes(storageVolume.getBaseStorageRootDir());
        if (totalSpaceInBytes != 0L) {
            Integer currentPercentage = storageVolume.getPercentageFull();
            int percentageFull = (int) ((totalSpaceInBytes - availableSpaceInBytes) * 100 / totalSpaceInBytes);
            if (!Integer.valueOf(percentageFull).equals(currentPercentage)) {
                LOG.trace("Update percentageFull for volume {}:{} to {}%", storageVolume.getId(), storageVolume.getName(), percentageFull);
                storageVolume.setPercentageFull(percentageFull);
                updatedVolumeFieldsBuilder.put("percentageFull", new SetFieldValueHandler<>(percentageFull));
                String locationMessagePart;
                if (storageVolume.isShared()) {
                    locationMessagePart = "";
                } else {
                    locationMessagePart = " on " + storageVolume.getStorageAgentId();
                }
                // check if it just crossed the threshold up or down
                if (percentageFull > FILL_UP_THRESHOLD && currentPercentage != null && currentPercentage <= FILL_UP_THRESHOLD) {
                    // it just crossed the threshold up
                    LOG.warn("Volume {} is {}% full and it just passed the threshold of {}%", storageVolume, percentageFull, FILL_UP_THRESHOLD);
                    String capacityNotification = "Volume " + storageVolume.getName() + locationMessagePart +
                            " just passed the fillup percentage threshold of " + FILL_UP_THRESHOLD + " and it currently is at " + percentageFull + "%";
                    capacityNotifier.sendNotification(
                            "Volume " + storageVolume.getName() + locationMessagePart + " is above fill up threshold",
                            capacityNotification);
                } else if (percentageFull <= FILL_UP_THRESHOLD && currentPercentage != null && currentPercentage > FILL_UP_THRESHOLD) {
                    // it just crossed the threshold down
                    LOG.info("Volume {} is {}% full and it just dropped below the threshold of {}%", storageVolume, percentageFull, FILL_UP_THRESHOLD);
                    String capacityNotification = "Volume " + storageVolume.getName() + locationMessagePart +
                            " just dropped below percentage threshold of " + FILL_UP_THRESHOLD + " and it currently is at " + percentageFull + "%";
                    capacityNotifier.sendNotification(
                            "Volume " + storageVolume.getName() + locationMessagePart + " is below the threshold now",
                            capacityNotification);
                }
            }
        }

        return updatedVolumeFieldsBuilder.build();
    }

    private long calculateTotalSpaceInBytes(String storageDirName) {
        return getFileStore(storageDirName)
                .map(fs -> {
                    try {
                        return fs.getTotalSpace();
                    } catch (IOException e) {
                        LOG.error("Error trying to get total storage space {}", storageDirName, e);
                        return 0L;
                    }
                })
                .orElse(0L)
                ;

    }

    private long calculateAvailableStorageSpaceInBytes(String storageDirName) {
        return getFileStore(storageDirName)
                .map(fs -> {
                    try {
                        return fs.getUsableSpace();
                    } catch (IOException e) {
                        LOG.error("Error trying to get usable storage space {}", storageDirName, e);
                        return 0L;
                    }
                })
                .orElse(0L)
                ;
    }

    private Optional<FileStore> getFileStore(String storageDirName) {
        try {
            java.nio.file.Path storagePath = Paths.get(storageDirName);
            if (Files.notExists(storagePath)) {
                Files.createDirectories(storagePath);
            }
            return Optional.of(Files.getFileStore(storagePath));
        } catch (Exception e) {
            LOG.error("Access error for storage {}", storageDirName, e);
            return Optional.empty();
        }
    }

}
