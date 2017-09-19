package org.janelia.jacsstorage.model.support;

/**
 * @param <T> field value type
 */
public class AppendFieldValueHandler<T> extends AbstractEntityFieldValueHandler<T> {
    public AppendFieldValueHandler(T fieldValue) {
        super(fieldValue);
    }
}
