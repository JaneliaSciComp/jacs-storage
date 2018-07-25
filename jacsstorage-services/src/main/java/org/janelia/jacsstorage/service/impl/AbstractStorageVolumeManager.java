package org.janelia.jacsstorage.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

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
    public JacsStorageVolume getVolumeById(Number volumeId) {
        return storageVolumeDao.findById(volumeId);
    }

    @TimedMethod
    @Override
    public JacsStorageVolume updateVolumeInfo(JacsStorageVolume storageVolume) {
        JacsStorageVolume currentVolumeInfo;

        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
        if (storageVolume.hasId()) {
            currentVolumeInfo = storageVolumeDao.findById(storageVolume.getId());
            Preconditions.checkArgument(currentVolumeInfo != null, "Invalid storage volume ID: " + storageVolume.getId());
        } else {
            currentVolumeInfo = storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(
                    storageVolume.getStorageHost(),
                    storageVolume.getName()
            );
        }
        if (currentVolumeInfo.getStorageRootDir() == null ||
                (StringUtils.isNotBlank(storageVolume.getStorageRootDir()) && !storageVolume.getStorageRootDir().equals(currentVolumeInfo.getStorageRootDir()))) {
            currentVolumeInfo.setStorageRootDir(storageVolume.getStorageRootDir());
            updatedVolumeFieldsBuilder.put("storageRootDir", new SetFieldValueHandler<>(currentVolumeInfo.getStorageRootDir()));
        }
        if (currentVolumeInfo.getStoragePathPrefix() == null ||
                (StringUtils.isNotBlank(storageVolume.getStoragePathPrefix()) && !storageVolume.getStoragePathPrefix().equals(currentVolumeInfo.getStoragePathPrefix()))) {
            currentVolumeInfo.setStoragePathPrefix(storageVolume.getStoragePathPrefix());
            updatedVolumeFieldsBuilder.put("storagePathPrefix", new SetFieldValueHandler<>(currentVolumeInfo.getStoragePathPrefix()));
        }
        if (storageVolume.isNotShared()) {
            storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getStorageRootDir()));
            if (!storageVolume.getAvailableSpaceInBytes().equals(currentVolumeInfo.getAvailableSpaceInBytes())) {
                LOG.debug("Update availableSpace for volume {} to {} bytes", currentVolumeInfo.getName(), storageVolume.getAvailableSpaceInBytes());
                currentVolumeInfo.setAvailableSpaceInBytes(storageVolume.getAvailableSpaceInBytes());
                updatedVolumeFieldsBuilder.put("availableSpaceInBytes", new SetFieldValueHandler<>(currentVolumeInfo.getAvailableSpaceInBytes()));
                if (storageVolume.hasPercentageAvailable()) {
                    currentVolumeInfo.setPercentageFull(storageVolume.getPercentageFull());
                    updatedVolumeFieldsBuilder.put("percentageFull", new SetFieldValueHandler<>(currentVolumeInfo.getPercentageFull()));
                    if (currentVolumeInfo.getPercentageFull() > FILL_UP_THRESHOLD) {
                        LOG.warn("Volume {} is {}% full", currentVolumeInfo, currentVolumeInfo.getPercentageFull());
                        String capacityNotification = "Volume " + currentVolumeInfo.getName() + " is " + currentVolumeInfo.getPercentageFull() + "% full";
                        capacityNotifier.sendNotification(capacityNotification, capacityNotification);
                    }
                }
            }
            if (StringUtils.isNotBlank(storageVolume.getStorageServiceURL()) &&
                    (StringUtils.isBlank(currentVolumeInfo.getStorageServiceURL()) ||
                            !storageVolume.getStorageServiceURL().equals(currentVolumeInfo.getStorageServiceURL()))) {
                currentVolumeInfo.setStorageServiceURL(storageVolume.getStorageServiceURL());
                updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceURL()));
            }
        } else if (currentVolumeInfo.isNotShared()) {
            // if somehow the current volume is not shared make it shared
            currentVolumeInfo.setShared(true);
            currentVolumeInfo.setStorageServiceURL(null);
            currentVolumeInfo.setStorageHost(null);
            updatedVolumeFieldsBuilder.put("shared", new SetFieldValueHandler<>(currentVolumeInfo.isShared()));
            updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceURL()));
            updatedVolumeFieldsBuilder.put("storageHost", new SetFieldValueHandler<>(currentVolumeInfo.getStorageHost()));
        }
        if (!currentVolumeInfo.hasTags() && storageVolume.hasTags() ||
                currentVolumeInfo.hasTags() && !storageVolume.hasTags() ||
                currentVolumeInfo.hasTags() && storageVolume.hasTags() && !ImmutableSet.copyOf(currentVolumeInfo.getStorageTags()).equals(ImmutableSet.copyOf(storageVolume.getStorageTags()))) {
            currentVolumeInfo.setStorageTags(storageVolume.getStorageTags());
            updatedVolumeFieldsBuilder.put("storageTags", new SetFieldValueHandler<>(currentVolumeInfo.getStorageTags()));
        }
        if (!currentVolumeInfo.hasPermissions() && storageVolume.hasPermissions() ||
                currentVolumeInfo.hasPermissions() && !storageVolume.hasPermissions() ||
                currentVolumeInfo.hasPermissions() && storageVolume.hasPermissions() && !currentVolumeInfo.getVolumePermissions().equals(storageVolume.getVolumePermissions())) {
            currentVolumeInfo.setVolumePermissions(storageVolume.getVolumePermissions());
            updatedVolumeFieldsBuilder.put("volumePermissions", new SetFieldValueHandler<>(currentVolumeInfo.getVolumePermissions()));
        }
        if (storageVolume.getQuotaFailPercent() != null && !storageVolume.getQuotaFailPercent().equals(currentVolumeInfo.getQuotaFailPercent())) {
            currentVolumeInfo.setQuotaFailPercent(storageVolume.getQuotaFailPercent());
            updatedVolumeFieldsBuilder.put("quotaFailPercent", new SetFieldValueHandler<>(currentVolumeInfo.getQuotaFailPercent()));
        }
        if (storageVolume.getQuotaWarnPercent() != null && !storageVolume.getQuotaWarnPercent().equals(currentVolumeInfo.getQuotaWarnPercent())) {
            currentVolumeInfo.setQuotaWarnPercent(storageVolume.getQuotaWarnPercent());
            updatedVolumeFieldsBuilder.put("quotaWarnPercent", new SetFieldValueHandler<>(currentVolumeInfo.getQuotaWarnPercent()));
        }
        if (StringUtils.isNotBlank(storageVolume.getSystemUsageFile()) && !storageVolume.getSystemUsageFile().equals(currentVolumeInfo.getSystemUsageFile())) {
            currentVolumeInfo.setSystemUsageFile(storageVolume.getSystemUsageFile());
            updatedVolumeFieldsBuilder.put("systemUsageFile", new SetFieldValueHandler<>(currentVolumeInfo.getSystemUsageFile()));
        }
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = updatedVolumeFieldsBuilder.build();
        if (!updatedVolumeFields.isEmpty()) {
            storageVolumeDao.update(currentVolumeInfo, updatedVolumeFieldsBuilder.build());
        }
        return currentVolumeInfo;
    }

    protected long getAvailableStorageSpaceInBytes(String storageDirName) {
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

    protected Optional<FileStore> getFileStore(String storageDirName) {
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
