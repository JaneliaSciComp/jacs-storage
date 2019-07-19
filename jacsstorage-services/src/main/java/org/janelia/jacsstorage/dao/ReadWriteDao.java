package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;

import java.util.List;
import java.util.Map;

/**
 * Read/Write data access spec.
 *
 * @param <T> entity type
 */
public interface ReadWriteDao<T extends BaseEntity> extends ReadOnlyDao<T> {
    void save(T entity);
    void saveAll(List<T> entities);
    T update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate);
    void delete(T entity);
}
