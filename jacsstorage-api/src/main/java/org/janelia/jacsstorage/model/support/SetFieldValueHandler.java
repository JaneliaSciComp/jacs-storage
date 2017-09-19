package org.janelia.jacsstorage.model.support;

/**
 * @param <T> field value type
 */
public class SetFieldValueHandler<T> extends AbstractEntityFieldValueHandler<T> {
    public SetFieldValueHandler(T fieldValue) {
        super(fieldValue);
    }
}
