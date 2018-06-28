package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigValueResolver;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class StorageVolumeBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(StorageVolumeBootstrapper.class);

    private final StorageVolumeManager storageVolumeManager;
    private final ApplicationConfig applicationConfig;
    private final String storageHost;
    private final List<String> bootstrappedVolumeNames;
    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

    @Inject
    public StorageVolumeBootstrapper(@LocalInstance StorageVolumeManager storageVolumeManager,
                                     @ApplicationProperties ApplicationConfig applicationConfig,
                                     @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                     @PropertyValue(name = "StorageAgent.BootstrappedVolumes") List<String> bootstrappedVolumeNames) {
        this.storageVolumeManager = storageVolumeManager;
        this.applicationConfig = applicationConfig;
        this.storageHost = StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
        this.bootstrappedVolumeNames = bootstrappedVolumeNames;
    }

    @TimedMethod
    public List<JacsStorageVolume> initializeStorageVolumes() {
        // initialize the list of specified volumes plus the overflow volume
        return Stream.concat(bootstrappedVolumeNames.stream(), Stream.of(JacsStorageVolume.OVERFLOW_VOLUME))
                .map(volumeName -> {
                    LOG.info("Bootstrap volume {}", volumeName);
                    JacsStorageVolume storageVolume = createVolumeInfo(volumeName);
                    return storageVolumeManager.updateVolumeInfo(storageVolume);
                })
                .collect(Collectors.toList());
    }

    private JacsStorageVolume createVolumeInfo(String volumeName) {
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

}
