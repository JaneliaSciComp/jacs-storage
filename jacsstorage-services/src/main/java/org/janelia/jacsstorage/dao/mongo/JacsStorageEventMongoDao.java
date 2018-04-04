package org.janelia.jacsstorage.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;

import javax.inject.Inject;
import java.util.Map;

/**
 * Mongo based implementation of JacsStorageEventDao.
 */
public class JacsStorageEventMongoDao extends AbstractMongoDao<JacsStorageEvent> implements JacsStorageEventDao {
    @Inject
    public JacsStorageEventMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
    }

    @Override
    public void update(JacsStorageEvent entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        throw new UnsupportedOperationException("Update not supported for storage event");
    }
}
