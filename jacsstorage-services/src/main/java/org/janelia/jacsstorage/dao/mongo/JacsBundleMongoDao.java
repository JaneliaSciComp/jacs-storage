package org.janelia.jacsstorage.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Mongo based implementation of JacsBundleDao.
 */
public class JacsBundleMongoDao extends AbstractMongoDao<JacsBundle> implements JacsBundleDao {

    @Inject
    public JacsBundleMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    public void update(JacsBundle entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> fieldsWithUpdatedDate = new LinkedHashMap<>(fieldsToUpdate);
        entity.setModified(new Date());
        fieldsWithUpdatedDate.put("modificationDate", new SetFieldValueHandler<>(entity.getModified()));
        super.update(entity, fieldsWithUpdatedDate);
    }

}
