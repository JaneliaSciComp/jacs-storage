package org.janelia.jacsstorage.model.support;

/**
 * @param <T> field value type
 */
public interface EntityFieldValueHandler<T> {
    T getFieldValue();
}
