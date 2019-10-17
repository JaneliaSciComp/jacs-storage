package org.janelia.jacsstorage.service;

import java.util.List;

import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public interface StorageVolumeManager {
    JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume);
    JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageAgentId);
    JacsStorageVolume getVolumeById(Number volumeId);
    List<JacsStorageVolume> findVolumes(StorageQuery storageQuery);
    JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume);
}
