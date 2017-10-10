package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.Optional;

public interface JacsStorageVolumeDao extends ReadWriteDao<JacsStorageVolume> {
    Optional<JacsStorageVolume> findStorageByLocation(String location);
    /**
     * Search the storage volume by location and if not found create one.
     *
     * @param location
     * @return
     */
    JacsStorageVolume getStorageByLocationAndCreateIfNotFound(String location);
}
