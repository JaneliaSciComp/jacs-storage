package org.janelia.jacsstorage.service.localservice;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LocalInstance
public class LocalStorageUsageManager implements StorageUsageManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageUsageManager.class);

    private final String storageAgentId;
    private final StorageVolumeManager storageVolumeManager;
    private final String quotaProxyUser;

    @Inject
    public LocalStorageUsageManager(@LocalInstance StorageVolumeManager storageVolumeManager,
                                    @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                    @PropertyValue(name = "StorageAgent.StoragePortNumber") String storagePort,
                                    @PropertyValue(name = "Storage.QuotaProxyUser", defaultValue = "jacs") String quotaProxyUser) {
        this.storageVolumeManager = storageVolumeManager;
        this.storageAgentId = NetUtils.createStorageHostId(
                StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName()),
                storagePort
        );
        this.quotaProxyUser = quotaProxyUser;
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByVolumeId(Number volumeId) {
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(volumeId);
        if (storageVolume == null) {
            LOG.warn("No volume found for {}", volumeId);
            throw new IllegalArgumentException("No volume found for " + volumeId);
        }
        return getVolumeUsage(storageVolume);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByVolumeIdForUser(Number volumeId, String username) {
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(volumeId);
        if (storageVolume == null) {
            LOG.warn("No volume found for {}", volumeId);
            throw new IllegalArgumentException("No volume found for " + volumeId);
        }
        return getVolumeUsageForUser(storageVolume, username);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByVolumeName(String volumeName) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.findVolumes(
                new StorageQuery()
                        .setStorageName(volumeName)
                        .setAccessibleOnAgent(storageAgentId)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        } else if (localVolumes.size() > 1) {
            LOG.debug("More than one storage volumes found for {} on {} -> {}", volumeName, storageAgentId, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsage(storageVolume);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByVolumeNameForUser(String volumeName, String username) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.findVolumes(
                new StorageQuery()
                        .setStorageName(volumeName)
                        .setAccessibleOnAgent(storageAgentId)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        } else if (localVolumes.size() > 1) {
            LOG.debug("More than one storage volumes found for {} on {} -> {}", volumeName, storageAgentId, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsageForUser(storageVolume, username);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByStoragePath(String storagePath) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.findVolumes(
                new StorageQuery()
                        .setDataStoragePath(storagePath)
                        .setAccessibleOnAgent(storageAgentId)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        } else if (localVolumes.size() > 1) {
            LOG.debug("More than one storage volumes found for {} on {} -> {}", storagePath, storageAgentId, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsage(storageVolume);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByStoragePathForUser(String storagePath, String username) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.findVolumes(
                new StorageQuery()
                        .setDataStoragePath(storagePath)
                        .setAccessibleOnAgent(storageAgentId)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        } else if (localVolumes.size() > 1) {
            LOG.debug("More than one storage volumes found for {} on {} -> {}", storagePath, storageAgentId, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsageForUser(storageVolume, username);
    }

    private UsageData getVolumeUsageForUser(JacsStorageVolume storageVolume, String username) {
        String userGroupId = getGroupNameForUserUsingDirHierarchy(storageVolume, username);
        if (userGroupId == null) {
            return UsageData.EMPTY;
        }
        List<UsageData> usageReport = getVolumeUsage(storageVolume);
        return usageReport.stream()
                .filter(usageData -> userGroupId.equalsIgnoreCase(usageData.getGroupId()))
                .filter(usageData -> StringUtils.isBlank(quotaProxyUser) || quotaProxyUser.equals(usageData.getUserProxy()))
                .reduce((ud1, ud2) -> ud1.add(ud2))
                .orElse(UsageData.EMPTY);
    }

    /**
     * This assumes that the directory structure is as follows:
     * <volume-path>/<group-name>/<user-name>
     * with symbolic links from <volume-path>/<user-name>
     *
     * @param storageVolume
     * @return
     */
    private String getGroupNameForUserUsingDirHierarchy(JacsStorageVolume storageVolume, String username) {
        Path storagePath;
        Path physicalStoragePath;
        try {
            storagePath = Paths.get(
                    ExprHelper.getConstPrefix(storageVolume.evalStorageRootDir(ImmutableMap.of("username", username)))
            );
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.warn("No storage path could be resolved on volume {} for user {}", storageVolume, username, e);
            } else {
                LOG.warn("No storage path could be resolved on volume {} for user {} : {}", storageVolume, username, e.getMessage());
            }
            return null;
        }
        try {
            physicalStoragePath = storagePath.toRealPath().toAbsolutePath();
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.warn("Storage path {} could not be resolved to a real path for user {} on volume {}", storagePath, username, storageVolume, e);
            } else {
                LOG.warn("Storage path {} could not be resolved to a real path for user {} : {}", storagePath, username, e.toString());
            }
            return null;
        }
        Pattern p = Pattern.compile(".*groups/(\\w+)/"+username+".*");
        Matcher m = p.matcher(physicalStoragePath.toString());
        if (m.matches()) {
            String userGroup = m.group(1);
            if (StringUtils.isBlank(userGroup)) {
                LOG.warn("Empty user group found for user {} from path {} on {}", username, physicalStoragePath, storageVolume);
                return null;
            } else {
                return userGroup;
            }
        } else {
            LOG.warn("Group could not be determined for user {} from path {} on {}", username, physicalStoragePath, storageVolume);
            return null;
        }
    }

    private List<UsageData> getVolumeUsage(JacsStorageVolume storageVolume) {
        List<UsageData> usageDataReport = new ArrayList<>();
        if (StringUtils.isBlank(storageVolume.getSystemUsageFile())) {
            LOG.warn("No system usage report was configured for {}", storageVolume);
            return usageDataReport;
        }
        Scanner scanner = null;
        try {
            LOG.info("Read system usage file {}", storageVolume.getSystemUsageFile());
            scanner = new Scanner(new File(storageVolume.getSystemUsageFile()));
            scanner.useDelimiter(",");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] cols = line.split(",");
                String groupName = cols.length > 0 ? cols[0].trim() : null;
                if (StringUtils.equalsIgnoreCase(groupName, "Lab") || StringUtils.equalsIgnoreCase(groupName, "FREE")) {
                    // Omit the header and the free space information
                    continue;
                }
                String spaceUsed = cols.length > 1 ? cols[1].trim() : null;
                String totalSpace = cols.length > 2 ? cols[2].trim() : null;
                String totalFiles = cols.length > 3 ? cols[3].trim() : null;
                String userProxy = cols.length > 4 ? cols[4].trim() : null;
                usageDataReport.add(new UsageData(groupName, spaceUsed, totalSpace, totalFiles,
                        storageVolume.getQuotaWarnPercent(), storageVolume.getQuotaFailPercent(),
                        userProxy));
            }
        } catch (Exception e) {
            LOG.error("Error reading system usage report from {} for {}", storageVolume.getSystemUsageFile(), storageVolume, e);
            throw new IllegalStateException("Error reading system usage report for " + storageVolume.getId());
        }
        return usageDataReport;
    }

}
