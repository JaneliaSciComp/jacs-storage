package org.janelia.jacsstorage.service.impl.localservice;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.ApplicationProperties;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.config.ApplicationConfigValueResolver;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageVolumeBootstrapper {

    private static final Logger LOG = LoggerFactory.getLogger(StorageVolumeBootstrapper.class);

    private final StorageVolumeManager storageVolumeManager;
    private final ApplicationConfig applicationConfig;
    private final List<String> bootstrappedVolumeNames;
    private final String storageAgentPlaceholderValue;
    private final ApplicationConfigValueResolver configValueResolver = new ApplicationConfigValueResolver();

    @Inject
    public StorageVolumeBootstrapper(@LocalInstance StorageVolumeManager storageVolumeManager,
                                     @ApplicationProperties ApplicationConfig applicationConfig,
                                     @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                     @PropertyValue(name = "StorageAgent.StoragePortNumber") String storagePort,
                                     @PropertyValue(name = "StorageAgent.BootstrappedVolumes") List<String> bootstrappedVolumeNames) {
        this.storageVolumeManager = storageVolumeManager;
        this.applicationConfig = applicationConfig;
        this.bootstrappedVolumeNames = bootstrappedVolumeNames;
        this.storageAgentPlaceholderValue = NetUtils.createStorageHostId(
                StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName()),
                storagePort,
                "_"
        );
    }

    @TimedMethod
    public List<JacsStorageVolume> initializeStorageVolumes(String storageAgentId) {
        // initialize the list of specified volumes plus the overflow volume
        return Stream.concat(bootstrappedVolumeNames.stream(), Stream.of(JacsStorageVolume.GENERIC_S3))
                .map(volumeName -> {
                    boolean shared;
                    JacsStorageType storageType;
                    if (JacsStorageVolume.GENERIC_S3.equals(volumeName)) {
                        shared = true;
                        storageType = JacsStorageType.S3;
                    } else {
                        shared = applicationConfig.getBooleanPropertyValue(
                                getVolumeConfigPropertyName(volumeName, "Shared"));
                        storageType = JacsStorageType.fromName(applicationConfig.getStringPropertyValue(
                                getVolumeConfigPropertyName(volumeName, "Storage"),
                                JacsStorageType.FILE_SYSTEM.name()));
                    }
                    LOG.info("Bootstrap {} volume {} at {}", shared ? "shared" : "local", volumeName, storageAgentId);
                    return storageVolumeManager.createStorageVolumeIfNotFound(volumeName, storageType, shared ? null : storageAgentId);
                })
                .filter(storageVolume -> storageVolume != null)
                .map(storageVolume -> {
                    fillVolumeInfo(storageVolume);
                    LOG.info("Update volume {}", storageVolume);
                    return storageVolumeManager.updateVolumeInfo(storageVolume.getId(), storageVolume);
                })
                .collect(Collectors.toList());
    }

    private void fillVolumeInfo(JacsStorageVolume storageVolume) {
        storageVolume.setStorageRootTemplate(applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(storageVolume.getName(), "RootDir")));
        storageVolume.setStorageVirtualPath(getStorageVirtualPath(storageVolume.getName()));
        storageVolume.setQuotaFailPercent(applicationConfig.getDoublePropertyValue(
                getVolumeConfigPropertyName(storageVolume.getName(), "QuotaFailPercent")));
        storageVolume.setQuotaWarnPercent(applicationConfig.getDoublePropertyValue(
                getVolumeConfigPropertyName(storageVolume.getName(), "QuotaWarnPercent")));
        storageVolume.setSystemUsageFile(applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(storageVolume.getName(), "SystemUsageFile")));
        storageVolume.setStorageTags(getStorageVolumeTags(storageVolume.getName()));
        storageVolume.setVolumePermissions(getStorageVolumePermissions(storageVolume.getName()));
        storageVolume.setActiveFlag(applicationConfig.getBooleanPropertyValue(
                getVolumeConfigPropertyName(storageVolume.getName(), "ActiveFlag"),
                true));
    }

    private String getVolumeConfigPropertyName(String volumeName, String configProperty) {
        return "StorageVolume." + volumeName + "." + configProperty;
    }

    private String getStorageVirtualPath(String volumeName) {
        String storagePathPrefix = applicationConfig.getStringPropertyValue(
                getVolumeConfigPropertyName(volumeName, "VirtualPath"));
        String resolvedStoragePathPrefix = configValueResolver.resolve(
                storagePathPrefix,
                (k) -> ImmutableMap.<String, String>builder()
                        .put("storageHost", storageAgentPlaceholderValue)
                        .build().get(k));
        return StringUtils.prependIfMissing(resolvedStoragePathPrefix, "/");
    }

    private List<String> getStorageVolumeTags(String volumeName) {
        List<String> tags = applicationConfig.getStringListPropertyValue(
                getVolumeConfigPropertyName(volumeName, "Tags"));
        return tags.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
    }

    private Set<JacsStoragePermission> getStorageVolumePermissions(String volumeName) {
        Stream<JacsStoragePermission> permissions;
        if (JacsStorageVolume.GENERIC_S3.equals(volumeName)) {
            permissions = Collections.singletonList(JacsStoragePermission.READ).stream();
        } else {
            permissions = applicationConfig.getStringListPropertyValue(
                    getVolumeConfigPropertyName(volumeName, "VolumePermissions")).stream()
                    .filter(StringUtils::isNotBlank)
                    .map(JacsStoragePermission::valueOf)
            ;
        }
        return permissions.collect(Collectors.toSet());
    }

}
