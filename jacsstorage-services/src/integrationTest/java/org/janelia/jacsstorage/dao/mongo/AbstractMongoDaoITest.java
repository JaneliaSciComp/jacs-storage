package org.janelia.jacsstorage.dao.mongo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.AbstractITest;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.ReadWriteDao;
import org.janelia.jacsstorage.dao.TimebasedIdGenerator;
import org.janelia.jacsstorage.dao.mongo.utils.RegistryHelper;
import org.janelia.jacsstorage.model.BaseEntity;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractMongoDaoITest extends AbstractITest {
    private static MongoClient testMongoClient;
    static ObjectMapperFactory testObjectMapperFactory = new ObjectMapperFactory();

    MongoDatabase testMongoDatabase;
    IdGenerator idGenerator;

    @BeforeClass
    @BeforeAll
    public static void setUpMongoClient() throws IOException {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(testObjectMapperFactory);
        MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder()
                .codecRegistry(CodecRegistries.fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        codecRegistry))
                .applyToConnectionPoolSettings(builder -> builder.maxConnectionIdleTime(60, TimeUnit.SECONDS))
                .applyConnectionString(new ConnectionString(integrationTestsConfig.getStringPropertyValue("MongoDB.ConnectionURL")))
                ;
        testMongoClient = MongoClients.create(mongoClientSettingsBuilder.build());
    }

    @Before
    @BeforeEach
    public final void setUpDaoResources() {
        idGenerator = new TimebasedIdGenerator(0);
        testMongoDatabase = testMongoClient.getDatabase(integrationTestsConfig.getStringPropertyValue("MongoDB.Database"));
    }

    protected <R extends BaseEntity> void deleteAll(ReadWriteDao<R> dao, List<R> es) {
        for (R e : es) {
            delete(dao, e);
        }
    }

    protected <R extends BaseEntity> void delete(ReadWriteDao<R> dao, R e) {
        if (e.getId() != null) {
            dao.delete(e);
        }
    }

    protected <R extends BaseEntity> R persistEntity(ReadWriteDao<R> dao, R e) {
        dao.save(e);
        return e;
    }

}
