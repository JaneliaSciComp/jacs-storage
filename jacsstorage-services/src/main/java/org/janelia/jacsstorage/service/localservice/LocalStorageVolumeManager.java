package org.janelia.jacsstorage.service.localservice;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigValueResolver;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.AbstractStorageVolumeManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.utils.NetUtils;

import javax.inject.Inject;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@LocalInstance
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private final ApplicationConfig applicationConfig;
    private final String storageHost;
    private final List<String> managedVolumes;
    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

    @Inject
    public LocalStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                     @ApplicationProperties ApplicationConfig applicationConfig,
                                     @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                     @PropertyValue(name = "StorageAgent.StorageVolumes") List<String> managedVolumes) {
        super(storageVolumeDao);
        this.applicationConfig = applicationConfig;
        this.storageHost = storageHost;
        this.managedVolumes = managedVolumes;
    }

    public List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery) {
        return Stream.concat(managedVolumes.stream(), Stream.of(JacsStorageVolume.OVERFLOW_VOLUME))
                .map(this::getVolumeInfo)
                .collect(Collectors.toList());
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

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

}