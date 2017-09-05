package org.janelia.jacsstorage.model.support;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;

public class EntityUtils {

    private static LoadingCache<Class<?>, MongoMapping> MONGO_MAPPING_CACHE_BUILDER = CacheBuilder.newBuilder()
            .maximumSize(20)
            .build(new CacheLoader<Class<?>, MongoMapping>() {
                @Override
                public MongoMapping load(Class<?> entityClass) throws Exception {
                    return loadMongoMapping(entityClass);
                }
            });

    private static MongoMapping loadMongoMapping(Class<?> objectClass) {
        MongoMapping mongoMapping = null;
        for(Class<?> clazz = objectClass; clazz != null; clazz = clazz.getSuperclass()) {
            if (clazz.isAnnotationPresent(MongoMapping.class)) {
                mongoMapping = clazz.getAnnotation(MongoMapping.class);
                break;
            }
        }
        return mongoMapping;
    }

    public static MongoMapping getMongoMapping(Class<?> objectClass) {
        try {
            return MONGO_MAPPING_CACHE_BUILDER.get(objectClass);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

}
