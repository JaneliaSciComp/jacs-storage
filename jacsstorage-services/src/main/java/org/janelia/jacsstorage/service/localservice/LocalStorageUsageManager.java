package org.janelia.jacsstorage.service.localservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@LocalInstance
public class LocalStorageUsageManager implements StorageUsageManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageUsageManager.class);

    private final String storageHost;
    private final StorageVolumeManager storageVolumeManager;

    @Inject
    public LocalStorageUsageManager(@LocalInstance StorageVolumeManager storageVolumeManager,
                                    @PropertyValue(name = "StorageAgent.StorageHost") String storageHost) {
        this.storageVolumeManager = storageVolumeManager;
        this.storageHost = StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());;
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByVolumeId(Number volumeId, JacsCredentials jacsCredentials) {
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
    public UsageData getUsageByVolumeIdForUser(Number volumeId, String username, JacsCredentials jacsCredentials) {
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
    public List<UsageData> getUsageByVolumeName(String volumeName, JacsCredentials jacsCredentials) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.getManagedVolumes(
                new StorageQuery().setStorageName(volumeName).setAccessibleOnHost(storageHost)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        } else if (localVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} on host {} -> {}", volumeName, storageHost, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsage(storageVolume);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByVolumeNameForUser(String volumeName, String username, JacsCredentials jacsCredentials) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.getManagedVolumes(
                new StorageQuery().setStorageName(volumeName).setAccessibleOnHost(storageHost)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", volumeName);
            throw new IllegalArgumentException("No volume found for " + volumeName);
        } else if (localVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} on host {} -> {}", volumeName, storageHost, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsageForUser(storageVolume, username);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<UsageData> getUsageByStoragePath(String storagePath, JacsCredentials jacsCredentials) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.getManagedVolumes(
                new StorageQuery().setDataStoragePath(storagePath).setAccessibleOnHost(storageHost)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        } else if (localVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} on host {} -> {}", storagePath, storageHost, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        return getVolumeUsage(storageVolume);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public UsageData getUsageByStoragePathForUser(String storagePath, String username, JacsCredentials jacsCredentials) {
        List<JacsStorageVolume> localVolumes = storageVolumeManager.getManagedVolumes(
                new StorageQuery().setDataStoragePath(storagePath).setAccessibleOnHost(storageHost)
        );
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", storagePath);
            throw new IllegalArgumentException("No volume found for " + storagePath);
        } else if (localVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} on host {} -> {}", storagePath, storageHost, localVolumes);
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
        Path storagePath = Paths.get(storageVolume.getStorageRootDir(), username);
        try {
            storagePath = storagePath.toRealPath().toAbsolutePath();
        } catch (IOException e) {
            LOG.warn("Storage path {} could not be resolved to a real path for user {} on volume {}",
                    storagePath, username, storageVolume, e);
            return null;
        }

        Pattern p = Pattern.compile(".*groups/(\\w+)/"+username+".*");
        Matcher m = p.matcher(storagePath.toString());
        if (m.matches()) {
            String userGroup = m.group(1);
            if (StringUtils.isBlank(userGroup)) {
                LOG.warn("Empty user group found for user {} from path {} on {}", username, storagePath, storageVolume);
                return null;
            } else {
                return userGroup;
            }
        } else {
            LOG.warn("Group could not be determined for user {} from path {} on {}", username, storagePath, storageVolume);
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
            scanner = new Scanner(new File(storageVolume.getSystemUsageFile()));
            scanner.useDelimiter(",");
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                String[] cols = line.split(",");
                int c = 0;
                String groupName = c < cols.length ? cols[c++].trim() : null;
                if (StringUtils.equalsIgnoreCase(groupName, "Lab") || StringUtils.equalsIgnoreCase(groupName, "FREE")) {
                    // Omit the header and the free space information
                    continue;
                }
                String spaceUsed = c < cols.length ? cols[c++].trim() : null;
                String totalSpace = c < cols.length ? cols[c++].trim() : null;
                String totalFiles = c < cols.length ? cols[c++].trim() : null;
                usageDataReport.add(new UsageData(groupName, spaceUsed, totalSpace, totalFiles,
                        storageVolume.getQuotaWarnPercent(), storageVolume.getQuotaFailPercent()));
            }
        } catch (Exception e) {
            LOG.error("Error reading system usage report from {} for {}", storageVolume.getSystemUsageFile(), storageVolume, e);
            throw new IllegalStateException("Error reading system usage report for " + storageVolume.getId());
        }
        return usageDataReport;
    }

}
