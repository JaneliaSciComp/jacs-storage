package org.janelia.jacsstorage.model.support;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;

public class EntityUtils {

    private static LoadingCache<Class<?>, PersistenceInfo> MONGO_MAPPING_CACHE_BUILDER = CacheBuilder.newBuilder()
            .maximumSize(20)
            .build(new CacheLoader<Class<?>, PersistenceInfo>() {
                @Override
                public PersistenceInfo load(Class<?> entityClass) throws Exception {
                    return loadMongoMapping(entityClass);
                }
            });

    private static PersistenceInfo loadMongoMapping(Class<?> objectClass) {
        PersistenceInfo persistenceInfo = null;
        for(Class<?> clazz = objectClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(PersistenceInfo.class)) {
                persistenceInfo = clazz.getAnnotation(PersistenceInfo.class);
                break;
            }
        }
        return persistenceInfo;
    }

    public static PersistenceInfo getMongoMapping(Class<?> objectClass) {
        try {
            return MONGO_MAPPING_CACHE_BUILDER.get(objectClass);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

}
