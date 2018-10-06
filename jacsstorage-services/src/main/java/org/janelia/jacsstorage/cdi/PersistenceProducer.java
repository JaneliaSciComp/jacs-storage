package org.janelia.jacsstorage.cdi;

import com.google.common.base.Splitter;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.cdi.qualifier.Cacheable;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.dao.CacheableJacsBundleDao;
import org.janelia.jacsstorage.dao.CacheableJacsStorageVolumeDao;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.dao.mongo.utils.RegistryHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class PersistenceProducer {
    private static final Logger LOG = LoggerFactory.getLogger(PersistenceProducer.class);
    private static final String DEFAULT_MONGO_AUTH_DB = "admin";

    @Inject
    @PropertyValue(name = "MongoDB.ConnectionURL")
    private String mongoConnectionURL;
    @Inject
    @PropertyValue(name = "MongoDB.ServerName")
    private String mongoServer;
    @Inject
    @PropertyValue(name = "MongoDB.Database")
    private String mongoDatabase;
    @Inject
    @PropertyValue(name = "MongoDB.AuthDatabase", defaultValue=DEFAULT_MONGO_AUTH_DB)
    private String authMongoDatabase;

    @ApplicationScoped
    @Produces
    public MongoClient createMongoClient(
            @PropertyValue(name = "MongoDB.ThreadsAllowedToBlockForConnectionMultiplier") int threadsAllowedToBlockMultiplier,
            @PropertyValue(name = "MongoDB.ConnectionsPerHost") int connectionsPerHost,
            @PropertyValue(name = "MongoDB.ConnectTimeout") int connectTimeout,
            @PropertyValue(name = "MongoDB.Username") String username,
            @PropertyValue(name = "MongoDB.Password") String password,
            ObjectMapperFactory objectMapperFactory) {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(objectMapperFactory);
        MongoClientOptions.Builder optionsBuilder =
                MongoClientOptions.builder()
                        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockMultiplier)
                        .connectionsPerHost(connectionsPerHost)
                        .connectTimeout(connectTimeout)
                        .codecRegistry(codecRegistry);
        if (StringUtils.isNotBlank(mongoServer)) {
            // use the server address
            List<ServerAddress> members = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(mongoServer)
                    .stream()
                    .map(ServerAddress::new)
                    .collect(Collectors.toList());
            if (StringUtils.isNotBlank(username)) {
                String credentialsDb = StringUtils.defaultIfBlank(authMongoDatabase, mongoDatabase);
                char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
                MongoCredential credential = MongoCredential.createCredential(username, credentialsDb, passwordChars);
                MongoClient m = new MongoClient(members, credential, optionsBuilder.build());
                LOG.info("Connected to MongoDB ({}@{}) as user {}", mongoDatabase, mongoServer, username);
                return m;
            } else {
                MongoClient m = new MongoClient(members, optionsBuilder.build());
                LOG.info("Connected to MongoDB ({}@{})", mongoDatabase, mongoServer);
                return m;
            }
        } else {
            // use the connection URI
            MongoClientURI mongoConnectionString = new MongoClientURI(mongoConnectionURL, optionsBuilder);
            LOG.info("Creating Mongo client {} using database {}", mongoConnectionString, mongoDatabase);
            return new MongoClient(mongoConnectionString);
        }
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
