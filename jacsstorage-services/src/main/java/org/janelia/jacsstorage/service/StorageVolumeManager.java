package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.List;

public interface StorageVolumeManager {
    JacsStorageVolume getVolumeById(Number volumeId);
    List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery);
    JacsStorageVolume updateVolumeInfo(JacsStorageVolume storageVolume);
}
