package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public interface JacsStorageVolumeDao extends ReadWriteDao<JacsStorageVolume> {
    /**
     * Search the storage volume by location and if not found create one.
     *
     * @param location
     * @return
     */
    JacsStorageVolume getStorageByLocation(String location);
}
