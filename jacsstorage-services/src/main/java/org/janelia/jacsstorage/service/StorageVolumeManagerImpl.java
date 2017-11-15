package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.DataInterval;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageVolumeManagerImpl implements StorageVolumeManager {

    private final JacsStorageVolumeDao storageVolumeDao;
    private final ApplicationConfig applicationConfig;
    private final String storageHost;
    private final List<String> managedVolumes;

    @Inject
    public StorageVolumeManagerImpl(JacsStorageVolumeDao storageVolumeDao,
                                    @ApplicationProperties ApplicationConfig applicationConfig,
                                    @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                    @PropertyValue(name = "StorageAgent.StorageVolumes") List<String> managedVolumes) {
        this.storageVolumeDao = storageVolumeDao;
        this.applicationConfig = applicationConfig;
        this.storageHost = storageHost;
        this.managedVolumes = managedVolumes;
    }

    @Override
    public List<JacsStorageVolume> findVolume(JacsStorageVolume volumeRef) {
        PageResult<JacsStorageVolume>  results = storageVolumeDao.findMatchingVolumes(volumeRef, new PageRequest());
        return results.getResultList();
    }

    public List<JacsStorageVolume> getManagedVolumes() {
        return Stream.concat(managedVolumes.stream(), Stream.of(JacsStorageVolume.OVERFLOW_VOLUME))
                .map(this::getVolumeInfo)
                .collect(Collectors.toList());
    }

    public JacsStorageVolume updateVolumeInfo(JacsStorageVolume storageVolume) {
        JacsStorageVolume currentVolumeInfo;

        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
        if (storageVolume.hasId()) {
            currentVolumeInfo = storageVolumeDao.findById(storageVolume.getId());
        } else {
            currentVolumeInfo = storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(
                    storageVolume.getStorageHost(),
                    storageVolume.getName()
            );
        }
        if (currentVolumeInfo.getVolumePath() == null) {
            currentVolumeInfo.setVolumePath(storageVolume.getVolumePath());
            updatedVolumeFieldsBuilder.put("volumePath", new SetFieldValueHandler<>(currentVolumeInfo.getVolumePath()));
        }
        if (!storageVolume.isShared()) {
            storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getVolumePath()));
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

    private JacsStorageVolume getVolumeInfo(String volumeName) {
        JacsStorageVolume storageVolume = new JacsStorageVolume();
        boolean shared;
        if (JacsStorageVolume.OVERFLOW_VOLUME.equals(volumeName)) {
            shared = true;
        } else {
            shared = applicationConfig.getBooleanPropertyValue("StorageVolume." + volumeName + ".Shared");
        }
        storageVolume.setName(volumeName);
        storageVolume.setShared(shared);
        storageVolume.setStorageHost(shared ? null : storageHost);
        storageVolume.setVolumePath(applicationConfig.getStringPropertyValue("StorageVolume." + volumeName + ".RootDir"));
        storageVolume.setStorageTags(getStorageVolumeTags(volumeName));
        storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getVolumePath()));
        return storageVolume;
    }

    private List<String> getStorageVolumeTags(String volumeName) {
        List<String> tags = applicationConfig.getStringListPropertyValue("StorageVolume." + volumeName + ".Tags");
        return tags.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
    }

    private long getAvailableStorageSpaceInBytes(String storageDirName) {
        try {
            java.nio.file.Path storagePath = Paths.get(storageDirName);
            if (Files.notExists(storagePath)) {
                Files.createDirectories(storagePath);
            }
            FileStore storageFileStore = Files.getFileStore(storagePath);
            long usableBytes = storageFileStore.getUsableSpace();
            return usableBytes;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
