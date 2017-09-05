package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.utils.Utils;

/**
 * Abstract base interface for data access.
 *
 * @param <T> entity type
 */
public abstract class AbstractDao<T extends BaseEntity> implements Dao<T> {
    protected Class<T> getEntityType() {
        return Utils.getGenericParameterType(this.getClass(), 0);
    }
}
