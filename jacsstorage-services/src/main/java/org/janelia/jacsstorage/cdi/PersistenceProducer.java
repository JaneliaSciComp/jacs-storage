package org.janelia.jacsstorage.cdi;

import com.google.common.base.Splitter;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.codecs.configuration.CodecRegistries;
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
import java.util.concurrent.TimeUnit;
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
    @PropertyValue(name = "MongoDB.ReplicaSet")
    private String mongoReplicaSet;
    @Inject
    @PropertyValue(name = "MongoDB.Database")
    private String mongoDatabase;
    @Inject
    @PropertyValue(name = "MongoDB.AuthDatabase", defaultValue=DEFAULT_MONGO_AUTH_DB)
    private String authMongoDatabase;

    @ApplicationScoped
    @Produces
    public MongoClient createMongoClient(
            @PropertyValue(name = "MongoDB.ConnectionsPerHost") int connectionsPerHost,
            @PropertyValue(name = "MongoDB.ConnectTimeoutInMillis") int connectTimeoutInMillis,
            @PropertyValue(name = "MongoDB.ConnectionWaitQueueSize") int connectionWaitQueueSize,
            @PropertyValue(name = "MongoDB.ConnectWaitTimeInSec") long connectWaitTimeInSec,
            @PropertyValue(name = "MongoDB.Username") String username,
            @PropertyValue(name = "MongoDB.Password") String password,
            ObjectMapperFactory objectMapperFactory) {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(objectMapperFactory);
        MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
                .codecRegistry(CodecRegistries.fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        codecRegistry))
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(connectionsPerHost)
                        .maxWaitQueueSize(connectionWaitQueueSize)
                        .maxWaitTime(connectWaitTimeInSec, TimeUnit.SECONDS)
                )
                .applyToSocketSettings(builder -> builder.connectTimeout(connectTimeoutInMillis, TimeUnit.MILLISECONDS))
                ;
        if (StringUtils.isNotBlank(mongoServer)) {
            List<ServerAddress> clusterMembers = Splitter.on(',')
                    .trimResults().omitEmptyStrings()
                    .splitToList(mongoServer).stream()
                    .map(ServerAddress::new)
                    .collect(Collectors.toList());
            mongoClientSettingsBuilder.applyToClusterSettings(builder -> builder.hosts(clusterMembers));
        } else {
            // use connection URL
            mongoClientSettingsBuilder.applyConnectionString(new ConnectionString(mongoConnectionURL));
        }
        if (StringUtils.isNotBlank(mongoReplicaSet)) {
            mongoClientSettingsBuilder.applyToClusterSettings(builder -> builder.requiredReplicaSetName(mongoReplicaSet));
        }
        if (StringUtils.isNotBlank(username)) {
            LOG.info("Connect to MongoDB ({}@{})", mongoDatabase, StringUtils.defaultIfBlank(mongoServer, mongoConnectionURL),
                    StringUtils.isBlank(username) ? "" : " as user " + username);
            String credentialsDb = StringUtils.defaultIfBlank(authMongoDatabase, mongoDatabase);
            char[] passwordChars = StringUtils.isBlank(password) ? null : password.toCharArray();
            mongoClientSettingsBuilder.credential(MongoCredential.createCredential(username, credentialsDb, passwordChars));
        }
        return MongoClients.create(mongoClientSettingsBuilder.build());
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
