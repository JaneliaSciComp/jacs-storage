package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsCredentials;

import java.util.List;

public interface StorageUsageManager {
    List<UsageData> getUsageByVolumeName(String volumeName);
    UsageData getUsageByVolumeNameForUser(String volumeName, String username);
    List<UsageData> getUsageByVolumeId(Number volumeId);
    UsageData getUsageByVolumeIdForUser(Number volumeId, String username);
    List<UsageData> getUsageByStoragePath(String storagePath);
    UsageData getUsageByStoragePathForUser(String storagePath, String username);
}
