package org.janelia.jacsstorage.cdi;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.dao.AbstractCacheableEntityByIdDao;
import org.janelia.jacsstorage.dao.CacheableJacsBundleDao;
import org.janelia.jacsstorage.dao.CacheableJacsStorageVolumeDao;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.dao.mongo.utils.RegistryHelper;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;

@ApplicationScoped
public class PersistenceProducer {

    @PropertyValue(name = "MongoDB.ConnectionURL")
    @Inject
    private String nmongoConnectionURL;
    @PropertyValue(name = "MongoDB.Database")
    @Inject
    private String mongoDatabase;

    @ApplicationScoped
    @Produces
    public MongoClient createMongoClient(
            @PropertyValue(name = "MongoDB.ThreadsAllowedToBlockForConnectionMultiplier") int threadsAllowedToBlockMultiplier,
            @PropertyValue(name = "MongoDB.ConnectionsPerHost") int connectionsPerHost,
            @PropertyValue(name = "MongoDB.ConnectTimeout") int connectTimeout,
            ObjectMapperFactory objectMapperFactory) {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(objectMapperFactory);
        MongoClientOptions.Builder optionsBuilder =
                MongoClientOptions.builder()
                        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockMultiplier)
                        .connectionsPerHost(connectionsPerHost)
                        .connectTimeout(connectTimeout)
                        .codecRegistry(codecRegistry);
        MongoClientURI mongoConnectionString = new MongoClientURI(nmongoConnectionURL, optionsBuilder);
        MongoClient mongoClient = new MongoClient(mongoConnectionString);
        return mongoClient;
    }

    @Produces
    public MongoDatabase createMongoDatabase(MongoClient mongoClient) {
        return mongoClient.getDatabase(mongoDatabase);
    }

    @ApplicationScoped
    @Produces
    @Cacheable
    public JacsStorageVolumeDao createCacheableJacsStorageVolumeDao(JacsStorageVolumeDao storageVolumeDao) {
        return new CacheableJacsStorageVolumeDao(storageVolumeDao);
    }

    @ApplicationScoped
    @Produces
    @Cacheable
    public JacsBundleDao createCacheableJacsBundleDao(JacsBundleDao bundleDao) {
        return new CacheableJacsBundleDao(bundleDao);
    }
}
