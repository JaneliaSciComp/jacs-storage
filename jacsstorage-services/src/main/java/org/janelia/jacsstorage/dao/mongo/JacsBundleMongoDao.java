package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

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

    @Override
    public JacsBundle findByOwnerAndName(String owner, String name) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(eq("owner", owner));
        filtersBuilder.add(eq("name", name));

        List<JacsBundle> results = find(and(filtersBuilder.build()),
                null,
                0,
                0,
                getEntityType());
        return results.isEmpty() ? null : results.get(0);
    }
}
