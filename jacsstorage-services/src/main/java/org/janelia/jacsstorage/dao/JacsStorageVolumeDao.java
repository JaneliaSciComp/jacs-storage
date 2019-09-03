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
     * Create a storage volume with the specified storage agent id if none exists.
     * If such volume exists it will simply return the existing entity.
     * The hostname can be empty in which case the method will create shared volume if no shared volume exists with the specified name.
     *
     * @param volumeName
     * @param agentId
     * @return
     */
    JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String agentId);
}
