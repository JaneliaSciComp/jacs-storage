package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;

import java.util.Collection;
import java.util.List;

/**
 * Read only data access spec.
 *
 * @param <T> entity type
 */
public interface ReadOnlyDao<T extends BaseEntity> extends Dao<T> {
    T findById(Number id);
    List<T> findByIds(Collection<Number> ids);
    PageResult<T> findAll(PageRequest pageRequest);
    long countAll();
}
