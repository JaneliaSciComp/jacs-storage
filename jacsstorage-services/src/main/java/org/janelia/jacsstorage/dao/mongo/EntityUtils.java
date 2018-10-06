package org.janelia.jacsstorage.dao.mongo;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.util.concurrent.ExecutionException;

class EntityUtils {

    private static LoadingCache<Class<?>, PersistenceInfo> MONGO_MAPPING_CACHE_BUILDER = CacheBuilder.newBuilder()
            .maximumSize(20)
            .build(new CacheLoader<Class<?>, PersistenceInfo>() {
                @Override
                public PersistenceInfo load(Class<?> entityClass) throws Exception {
                    return loadPersistenceInfo(entityClass);
                }
            });

    private static PersistenceInfo loadPersistenceInfo(Class<?> objectClass) {
        PersistenceInfo persistenceInfo = null;
        for(Class<?> clazz = objectClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(PersistenceInfo.class)) {
                persistenceInfo = clazz.getAnnotation(PersistenceInfo.class);
                break;
            }
        }
        return persistenceInfo;
    }

    static PersistenceInfo getPersistenceInfo(Class<?> objectClass) {
        try {
            return MONGO_MAPPING_CACHE_BUILDER.get(objectClass);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

}
