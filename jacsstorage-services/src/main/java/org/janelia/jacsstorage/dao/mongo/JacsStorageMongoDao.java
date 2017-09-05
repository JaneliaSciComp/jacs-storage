package org.janelia.jacsstorage.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorage;

import javax.inject.Inject;

/**
 * Mongo based implementation of JacsStorageDao.
 */
public class JacsStorageMongoDao extends AbstractMongoDao<JacsStorage> implements JacsStorageDao {
    @Inject
    public JacsStorageMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}
