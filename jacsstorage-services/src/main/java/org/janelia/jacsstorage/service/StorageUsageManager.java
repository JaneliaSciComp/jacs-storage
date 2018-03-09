package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsCredentials;

import java.util.List;

public interface StorageUsageManager {
    List<UsageData> getVolumeUsage(Number volumeId, JacsCredentials jacsCredentials);
    UsageData getVolumeUsageForUser(Number volumeId, String username, JacsCredentials jacsCredentials);
    List<UsageData> getUsageByStoragePath(String storagePath, JacsCredentials jacsCredentials);
    UsageData getUsageByStoragePathForUser(String storagePath, String username, JacsCredentials jacsCredentials);
}
