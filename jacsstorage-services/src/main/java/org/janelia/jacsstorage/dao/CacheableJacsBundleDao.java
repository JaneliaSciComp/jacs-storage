package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

/**
 * Cached data accessed by ID.
 */
public class CacheableJacsBundleDao extends AbstractCacheableEntityByIdDao<JacsBundle> implements JacsBundleDao {

    private JacsBundleDao dao;

    public CacheableJacsBundleDao(JacsBundleDao dao) {
        this.dao = dao;
    }

    @Override
    public JacsBundle findByOwnerKeyAndName(String ownerKey, String name) {
        return getDelegator().findByOwnerKeyAndName(ownerKey, name);
    }

    @Override
    public long countMatchingDataBundles(JacsBundle pattern) {
        return getDelegator().countMatchingDataBundles(pattern);
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        return getDelegator().findMatchingDataBundles(pattern, pageRequest);
    }

    @Override
    protected JacsBundleDao getDelegator() {
        return dao;
    }
}

