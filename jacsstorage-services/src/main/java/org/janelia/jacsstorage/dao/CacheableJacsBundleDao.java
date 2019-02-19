package org.janelia.jacsstorage.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

import java.util.concurrent.TimeUnit;

/**
 * Cached data accessed by ID.
 */
public class CacheableJacsBundleDao extends AbstractCacheableEntityByIdDao<JacsBundle> implements JacsBundleDao {

    private static final Cache<Number, JacsBundle> JACS_BUNDLE_CACHE = CacheBuilder.newBuilder()
            .maximumSize(100)
            .expireAfterAccess(5, TimeUnit.MINUTES)
            .build();

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

    @Override
    protected Cache<Number, JacsBundle> getCache() {
        return JACS_BUNDLE_CACHE;
    }
}
