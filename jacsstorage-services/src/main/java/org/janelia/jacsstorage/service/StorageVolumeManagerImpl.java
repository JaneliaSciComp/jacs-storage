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

public class StorageVolumeManagerImpl implements StorageVolumeManager {

    private final JacsStorageVolumeDao storageVolumeDao;
    private final ApplicationConfig applicationConfig;
    private final String storageHost;
    private final List<String> managedVolumes;
    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

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
        storageVolume.setStorageHost(shared ? null : getStorageHost());
        storageVolume.setStorageRootDir(applicationConfig.getStringPropertyValue("StorageVolume." + volumeName + ".RootDir"));
        storageVolume.setStoragePathPrefix(getStoragePathPrefix(volumeName));
        storageVolume.setStorageTags(getStorageVolumeTags(volumeName));
        storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getStorageRootDir()));
        return storageVolume;
    }

    private String getStoragePathPrefix(String volumeName) {
        String storagePathPrefix = applicationConfig.getStringPropertyValue("StorageVolume." + volumeName + ".PathPrefix");
        return configValueResolver.resolve(storagePathPrefix, ImmutableMap.<String, String>builder().putAll(applicationConfig.asMap()).put("storageHost", getStorageHost()).build());
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

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

}
