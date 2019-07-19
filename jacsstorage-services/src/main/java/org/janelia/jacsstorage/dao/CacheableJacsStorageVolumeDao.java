package org.janelia.jacsstorage.dao;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

/**
 * Cached data accessed by ID.
 */
public class CacheableJacsStorageVolumeDao extends AbstractCacheableEntityByIdDao<JacsStorageVolume> implements JacsStorageVolumeDao {

    private static final Cache<Number, JacsStorageVolume> JACS_VOLUME_CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

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
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String hostName) {
        return getDelegator().createStorageVolumeIfNotFound(volumeName, hostName);
    }

    @Override
    protected JacsStorageVolumeDao getDelegator() {
        return dao;
    }

    @Override
    protected Cache<Number, JacsStorageVolume> getCache() {
        return JACS_VOLUME_CACHE;
    }
}

