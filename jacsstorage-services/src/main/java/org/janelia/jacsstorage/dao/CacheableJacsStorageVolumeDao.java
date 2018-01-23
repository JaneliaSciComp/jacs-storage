package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

/**
 * Cached data accessed by ID.
 */
public class CacheableJacsStorageVolumeDao extends AbstractCacheableEntityByIdDao<JacsStorageVolume> implements JacsStorageVolumeDao {

    private JacsStorageVolumeDao dao;

    public CacheableJacsStorageVolumeDao(JacsStorageVolumeDao dao) {
        this.dao = dao;
    }

    @Override
    public Long countMatchingVolumes(StorageQuery storageQuery) {
        return getDelegator().countMatchingVolumes(storageQuery);
    }

    @Override
    public PageResult<JacsStorageVolume> findMatchingVolumes(StorageQuery storageQuery, PageRequest pageRequest) {
        return getDelegator().findMatchingVolumes(storageQuery, pageRequest);
    }

    @Override
    public JacsStorageVolume getStorageByHostAndNameAndCreateIfNotFound(String hostName, String volumeName) {
        return getDelegator().getStorageByHostAndNameAndCreateIfNotFound(hostName, volumeName);
    }

    @Override
    protected JacsStorageVolumeDao getDelegator() {
        return dao;
    }
}

