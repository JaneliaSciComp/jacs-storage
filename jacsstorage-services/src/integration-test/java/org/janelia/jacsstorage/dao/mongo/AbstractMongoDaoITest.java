package org.janelia.jacsstorage.dao.mongo;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoDatabase;
import org.bson.codecs.configuration.CodecRegistry;
import org.janelia.jacsstorage.AbstractITest;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.ReadWriteDao;
import org.janelia.jacsstorage.dao.mongo.utils.RegistryHelper;
import org.janelia.jacsstorage.dao.TimebasedIdGenerator;
import org.janelia.jacsstorage.model.BaseEntity;
import org.janelia.jacsstorage.model.jacsstorage.JacsVolume;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;

public abstract class AbstractMongoDaoITest<T extends BaseEntity> extends AbstractITest {
    private static MongoClient testMongoClient;
    protected static ObjectMapperFactory testObjectMapperFactory = ObjectMapperFactory.instance();

    protected MongoDatabase testMongoDatabase;
    protected IdGenerator idGenerator;

    @BeforeClass
    public static void setUpMongoClient() throws IOException {
        CodecRegistry codecRegistry = RegistryHelper.createCodecRegistry(testObjectMapperFactory);
        MongoClientOptions.Builder optionsBuilder = MongoClientOptions.builder().codecRegistry(codecRegistry).maxConnectionIdleTime(60000);
        MongoClientURI mongoConnectionString = new MongoClientURI(integrationTestsConfig.getProperty("MongoDB.ConnectionURL"), optionsBuilder);
        testMongoClient = new MongoClient(mongoConnectionString);
    }

    @Before
    public final void setUpDaoResources() {
        idGenerator = new TimebasedIdGenerator(0);
        testMongoDatabase = testMongoClient.getDatabase(integrationTestsConfig.getProperty("MongoDB.Database"));
    }

    protected void deleteAll(ReadWriteDao<T> dao, List<T> es) {
        for (T e : es) {
            delete(dao, e);
        }
    }

    protected void delete(ReadWriteDao<T> dao, T e) {
        if (e.getId() != null) {
            dao.delete(e);
        }
    }

    protected T persistEntity(ReadWriteDao<T> dao, T e) {
        dao.save(e);
        return e;
    }

}
