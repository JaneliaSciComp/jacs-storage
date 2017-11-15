package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.List;

public interface StorageVolumeManager {
    List<JacsStorageVolume> findVolume(JacsStorageVolume volumeRef);
    List<JacsStorageVolume> getManagedVolumes();
    JacsStorageVolume updateVolumeInfo(JacsStorageVolume storageVolume);
}
