package org.janelia.jacsstorage.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigValueResolver;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.utils.NetUtils;

import javax.inject.Inject;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class AbstractStorageVolumeManager implements StorageVolumeManager {

    protected final JacsStorageVolumeDao storageVolumeDao;

    @Inject
    public AbstractStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao) {
        this.storageVolumeDao = storageVolumeDao;
    }

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
        if (currentVolumeInfo.getStorageRootDir() == null) {
            currentVolumeInfo.setStorageRootDir(storageVolume.getStorageRootDir());
            updatedVolumeFieldsBuilder.put("storageRootDir", new SetFieldValueHandler<>(currentVolumeInfo.getStorageRootDir()));
        }
        if (currentVolumeInfo.getStoragePathPrefix() == null) {
            currentVolumeInfo.setStoragePathPrefix(storageVolume.getStoragePathPrefix());
            updatedVolumeFieldsBuilder.put("storagePathPrefix", new SetFieldValueHandler<>(currentVolumeInfo.getStoragePathPrefix()));
        }
        if (!storageVolume.isShared()) {
            storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getStorageRootDir()));
            if (!storageVolume.getAvailableSpaceInBytes().equals(currentVolumeInfo.getAvailableSpaceInBytes())) {
                currentVolumeInfo.setAvailableSpaceInBytes(storageVolume.getAvailableSpaceInBytes());
                updatedVolumeFieldsBuilder.put("availableSpaceInBytes", new SetFieldValueHandler<>(currentVolumeInfo.getAvailableSpaceInBytes()));
            }
            if (StringUtils.isNotBlank(storageVolume.getStorageServiceURL()) &&
                    (StringUtils.isBlank(currentVolumeInfo.getStorageServiceURL()) ||
                            !storageVolume.getStorageServiceURL().equals(currentVolumeInfo.getStorageServiceURL()))) {
                currentVolumeInfo.setStorageServiceURL(storageVolume.getStorageServiceURL());
                updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceURL()));
            }
            if (storageVolume.getStorageServiceTCPPortNo() != 0 &&
                    storageVolume.getStorageServiceTCPPortNo() != currentVolumeInfo.getStorageServiceTCPPortNo()) {
                currentVolumeInfo.setStorageServiceTCPPortNo(storageVolume.getStorageServiceTCPPortNo());
                updatedVolumeFieldsBuilder.put("storageServiceTCPPortNo", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceTCPPortNo()));
            }
        } else if (!currentVolumeInfo.isShared()) {
            // if somehow the current volume is not shared make it shared
            currentVolumeInfo.setShared(true);
            currentVolumeInfo.setStorageServiceURL(null);
            currentVolumeInfo.setStorageHost(null);
            currentVolumeInfo.setStorageServiceTCPPortNo(0);
            updatedVolumeFieldsBuilder.put("shared", new SetFieldValueHandler<>(currentVolumeInfo.isShared()));
            updatedVolumeFieldsBuilder.put("storageServiceURL", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceURL()));
            updatedVolumeFieldsBuilder.put("storageHost", new SetFieldValueHandler<>(currentVolumeInfo.getStorageHost()));
            updatedVolumeFieldsBuilder.put("storageServiceTCPPortNo", new SetFieldValueHandler<>(currentVolumeInfo.getStorageServiceTCPPortNo()));
        }
        if (!currentVolumeInfo.hasTags() && storageVolume.hasTags()) {
            currentVolumeInfo.setStorageTags(storageVolume.getStorageTags());
            updatedVolumeFieldsBuilder.put("volumeTags", new SetFieldValueHandler<>(currentVolumeInfo.getStorageTags()));
        }
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = updatedVolumeFieldsBuilder.build();
        if (!updatedVolumeFields.isEmpty()) {
            storageVolumeDao.update(currentVolumeInfo, updatedVolumeFieldsBuilder.build());
        }
        return currentVolumeInfo;
    }

    private long getAvailableStorageSpaceInBytes(String storageDirName) {
        try {
            return getFileStore(storageDirName).getUsableSpace();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private FileStore getFileStore(String storageDirName) {
        try {
            java.nio.file.Path storagePath = Paths.get(storageDirName);
            if (Files.notExists(storagePath)) {
                Files.createDirectories(storagePath);
            }
            return Files.getFileStore(storagePath);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
