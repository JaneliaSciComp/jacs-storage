package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigValueResolver;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@LocalInstance
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageVolumeManager.class);

    private final ApplicationConfig applicationConfig;
    private final String storageHost;
    private final List<String> managedVolumes;
    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

    @Inject
    public LocalStorageVolumeManager(@Cacheable JacsStorageVolumeDao storageVolumeDao,
                                     NotificationService capacityNotifier,
                                     @ApplicationProperties ApplicationConfig applicationConfig,
                                     @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                     @PropertyValue(name = "StorageAgent.StorageVolumes") List<String> managedVolumes) {
        super(storageVolumeDao, capacityNotifier);
        this.applicationConfig = applicationConfig;
        this.storageHost = StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
        this.managedVolumes = managedVolumes;
    }

    @TimedMethod
    @Override
    public Optional<JacsStorageVolume> getFullVolumeInfo(String volumeName) {
        if (managedVolumes.contains(volumeName)) {
            JacsStorageVolume storageVolume = getLocalVolumeInfo(volumeName);
            fillVolumeSpaceInfo(storageVolume);
            return Optional.of(storageVolume);
        } else {
            LOG.warn("Volume {} not available at {}", volumeName, storageHost);
            return Optional.empty();
        }
    }

    private void fillVolumeSpaceInfo(JacsStorageVolume storageVolume) {
        storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageVolume.getStorageRootDir()));
        long totalSpace = getTotalStorageSpaceInBytes(storageVolume.getStorageRootDir());
        if (totalSpace != 0) {
            storageVolume.setPercentageFull((int) ((totalSpace - storageVolume.getAvailableSpaceInBytes()) * 100 / totalSpace));
        } else {
            LOG.warn("Total space calculated is 0");
        }
    }

    @TimedMethod
    @Override
    public List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery) {
        LOG.trace("Query managed volumes using {}", storageQuery);
        return Stream.concat(managedVolumes.stream(), Stream.of(JacsStorageVolume.OVERFLOW_VOLUME))
                .map(this::getLocalVolumeInfo)
                .filter(sv -> StringUtils.isBlank(storageQuery.getDataStoragePath()) ||
                        storageQuery.getDataStoragePath().startsWith(sv.getStoragePathPrefix()) ||
                        storageQuery.getDataStoragePath().startsWith(sv.getStorageRootDir())
                )
                .filter(sv -> StringUtils.isBlank(storageQuery.getStoragePathPrefix()) ||
                        storageQuery.getStoragePathPrefix().equals(sv.getStoragePathPrefix())
                )
                .filter(sv -> StringUtils.isBlank(storageQuery.getStorageName()) ||
                        storageQuery.getStorageName().equals(sv.getName())
                )
                .filter(sv -> CollectionUtils.isEmpty(storageQuery.getStorageTags()) ||
                        sv.getStorageTags().containsAll(storageQuery.getStorageTags())
                )
                .sorted((v1, v2) -> {
                    int rootComparisonResult = v1.getStorageRootDir().compareTo(v2.getStorageRootDir());
                    if (rootComparisonResult == 0) {
                        return -v1.getStoragePathPrefix().compareTo(v2.getStoragePathPrefix());
                    } else {
                        return -rootComparisonResult; // order descending by root dir
                    }
                })
                .collect(Collectors.toList());
    }

    private JacsStorageVolume getLocalVolumeInfo(String volumeName) {
        JacsStorageVolume storageVolume = new JacsStorageVolume();
        boolean shared;
        if (JacsStorageVolume.OVERFLOW_VOLUME.equals(volumeName)) {
            shared = true;
        } else {
            shared = applicationConfig.getBooleanPropertyValue(
                    getVolumeConfigPropertyName(volumeName, "Shared"));
        }
        storageVolume.setName(volumeName);
        storageVolume.setShared(shared);
        storageVolume.setStorageHost(shared ? null : storageHost);
        storageVolume.setStorageRootDir(applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(volumeName, "RootDir")));
        storageVolume.setStoragePathPrefix(getStoragePathPrefix(volumeName));
        storageVolume.setStorageTags(getStorageVolumeTags(volumeName));
        storageVolume.setQuotaFailPercent(applicationConfig.getDoublePropertyValue(
                getVolumeConfigPropertyName(volumeName, "QuotaFailPercent")));
        storageVolume.setQuotaWarnPercent(applicationConfig.getDoublePropertyValue(
                getVolumeConfigPropertyName(volumeName, "QuotaWarnPercent")));
        storageVolume.setSystemUsageFile(applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(volumeName, "SystemUsageFile")));
        return storageVolume;
    }

    private String getVolumeConfigPropertyName(String volumeName, String configProperty) {
        return "StorageVolume." + volumeName + "." + configProperty;
    }

    private String getStoragePathPrefix(String volumeName) {
        String storagePathPrefix = applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(volumeName, "PathPrefix"));
        String resolvedStoragePathPrefix = configValueResolver.resolve(
                storagePathPrefix,
                ImmutableMap.<String, String>builder()
                        .put("storageHost", storageHost)
                        .build());
        return StringUtils.prependIfMissing(resolvedStoragePathPrefix, "/");
    }

    private List<String> getStorageVolumeTags(String volumeName) {
        List<String> tags = applicationConfig.getStringListPropertyValue(
                getVolumeConfigPropertyName(volumeName, "Tags"));
        return tags.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
    }

    private long getTotalStorageSpaceInBytes(String storageDirName) {
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

}
