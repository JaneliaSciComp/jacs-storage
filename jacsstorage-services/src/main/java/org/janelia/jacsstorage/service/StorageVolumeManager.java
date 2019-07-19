package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;

import java.util.List;
import java.util.Optional;

public interface StorageVolumeManager {
    JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume);
    JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageHost);
    JacsStorageVolume getVolumeById(Number volumeId);
    List<JacsStorageVolume> findVolumes(StorageQuery storageQuery);
    JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume);
}
