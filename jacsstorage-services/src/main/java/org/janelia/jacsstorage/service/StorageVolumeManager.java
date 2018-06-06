package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;

import java.util.List;
import java.util.Optional;

public interface StorageVolumeManager {
    JacsStorageVolume getVolumeById(Number volumeId);
    Optional<JacsStorageVolume> getFullVolumeInfo(String volumeName);
    List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery);
    JacsStorageVolume updateVolumeInfo(JacsStorageVolume storageVolume);
}
