package org.janelia.jacsstorage.dao;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * Cached data accessed by ID.
 *
 * @param <T> entity type
 */
public abstract class AbstractCacheableEntityByIdDao<T extends BaseEntity>
        extends AbstractDao<T>
        implements ReadWriteDao<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractCacheableEntityByIdDao.class);

    protected abstract ReadWriteDao<T> getDelegator();

    protected abstract Cache<Number, T> getCache();

    @Override
    public void save(T entity) {
        getDelegator().save(entity);
        invalidateCache(entity);
    }

    @Override
    public void saveAll(List<T> entities) {
        getDelegator().saveAll(entities);
        entities.forEach(e -> invalidateCache(e));
    }

    @Override
    public void update(T entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        getDelegator().update(entity, fieldsToUpdate);
        invalidateCache(entity);
    }

    @Override
    public void delete(T entity) {
        getDelegator().delete(entity);
        invalidateCache(entity);
    }

    @Override
    public T findById(Number id) {
        try {
            return getCache().get(id, new Callable<T>() {
                @Override
                public T call() {
                    return getDelegator().findById(id);
                }
            });
        } catch (Exception e) {
            LOG.warn("No entity of type {} found for {}", getEntityType(), id, e);
            return null;
        }
    }

    @Override
    public List<T> findByIds(Collection<Number> ids) {
        return getDelegator().findByIds(ids);
    }

    @Override
    public PageResult<T> findAll(PageRequest pageRequest) {
        return getDelegator().findAll(pageRequest);
    }

    @Override
    public long countAll() {
        return getDelegator().countAll();
    }

    private void invalidateCache(T entity) {
        getCache().invalidate(entity.getId());
    }
}

