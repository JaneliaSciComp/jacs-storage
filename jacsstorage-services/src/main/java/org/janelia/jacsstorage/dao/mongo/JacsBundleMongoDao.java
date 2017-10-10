package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
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
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.unique(true);
        mongoCollection.createIndex(Indexes.ascending("owner", "name"), indexOptions);
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

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (StringUtils.isNotBlank(pattern.getOwner())) {
            filtersBuilder.add(eq("owner", pattern.getOwner()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(eq("name", pattern.getName()));
        }
        Number storageNumberId = pattern.getStorageVolumeId();
        if (storageNumberId != null) {
            filtersBuilder.add(eq("storageVolumeId", storageNumberId));
        }
        List<JacsBundle> results = find(and(filtersBuilder.build()),
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());

        return new PageResult<>(pageRequest, results);
    }
}
