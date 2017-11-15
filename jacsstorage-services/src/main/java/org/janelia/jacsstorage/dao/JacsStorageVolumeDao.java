package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.DataInterval;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.List;
import java.util.Optional;

public interface JacsStorageVolumeDao extends ReadWriteDao<JacsStorageVolume> {
    /**
     * Find matching volume reference.
     * @param pattern
     * @param pageRequest
     * @return
     */
    Long countMatchingVolumes(JacsStorageVolume pattern);
    /**
     * Find matching volume reference.
     * @param pattern
     * @param pageRequest
     * @return
     */
    PageResult<JacsStorageVolume> findMatchingVolumes(JacsStorageVolume pattern, PageRequest pageRequest);
    /**
     * Search the storage volume by hostName and volumeName and if not found create one.
     *
     * @param hostName
     * @param volumeName
     * @return
     */
    JacsStorageVolume getStorageByHostAndNameAndCreateIfNotFound(String hostName, String volumeName);
}
