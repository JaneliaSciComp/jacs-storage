package org.janelia.jacsstorage.utils;

import java.lang.reflect.ParameterizedType;

public class Utils {
    @SuppressWarnings("unchecked")
    public static <T> Class<T> getGenericParameterType(Class<?> parameterizedClass, int paramIndex) {
        return (Class<T>)((ParameterizedType)parameterizedClass.getGenericSuperclass()).getActualTypeArguments()[paramIndex];
    }
}
