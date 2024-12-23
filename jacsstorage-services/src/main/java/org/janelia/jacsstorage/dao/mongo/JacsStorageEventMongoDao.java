package org.janelia.jacsstorage.dao.mongo;

import java.util.Map;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;

/**
 * Mongo based implementation of JacsStorageEventDao.
 */
@ApplicationScoped
public class JacsStorageEventMongoDao extends AbstractMongoDao<JacsStorageEvent> implements JacsStorageEventDao {
    @Inject
    public JacsStorageEventMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
    }

    @Override
    public JacsStorageEvent update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        throw new UnsupportedOperationException("Update not supported for storage event");
    }
}
