package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public interface JacsStorageVolumeDao extends ReadWriteDao<JacsStorageVolume> {
    /**
     * Find matching volume reference.
     * @param pattern
     * @param pageRequest
     * @return
     */
    Long countMatchingVolumes(StorageQuery storageQuery);
    /**
     * Find matching volume reference.
     * @param pattern
     * @param pageRequest
     * @return
     */
    PageResult<JacsStorageVolume> findMatchingVolumes(StorageQuery storageQuery, PageRequest pageRequest);
    /**
     * Search the storage volume by hostName and volumeName and if not found create one.
     *
     * @param hostName
     * @param volumeName
     * @return
     */
    JacsStorageVolume getStorageByHostAndNameAndCreateIfNotFound(String hostName, String volumeName);
}
