package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
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
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.util.Date;
import java.util.Iterator;
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
        mongoCollection.createIndex(Indexes.ascending("ownerKey", "name"), indexOptions);
    }

    @Override
    public void update(JacsBundle entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> fieldsWithUpdatedDate = new LinkedHashMap<>(fieldsToUpdate);
        entity.setModified(new Date());
        fieldsWithUpdatedDate.put("modified", new SetFieldValueHandler<>(entity.getModified()));
        super.update(entity, fieldsWithUpdatedDate);
    }

    @Override
    public JacsBundle findByOwnerKeyAndName(String ownerKey, String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(ownerKey));
        Preconditions.checkArgument(StringUtils.isNotBlank(name));

        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(eq("ownerKey", ownerKey));
        filtersBuilder.add(eq("name", name));

        Iterator<JacsBundle> resultsItr = findIterable(and(filtersBuilder.build()),
                null,
                0,
                0,
                getEntityType()).iterator();
        return resultsItr.hasNext() ? resultsItr.next() : null;
    }

    @Override
    public long countMatchingDataBundles(JacsBundle pattern) {
        Bson bsonFilter = getBsonFilter(pattern);
        return countAggregate(bsonFilter, getAggregationOps(pattern));
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        Bson bsonFilter = getBsonFilter(pattern);
        List<JacsBundle> results = aggregateAsList(bsonFilter,
                getAggregationOps(pattern),
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                (int) pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());

        return new PageResult<>(pageRequest, results);
    }

    private Bson getBsonFilter(JacsBundle pattern) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(eq("_id", pattern.getId()));
        }
        if (StringUtils.isNotBlank(pattern.getOwnerKey())) {
            filtersBuilder.add(eq("ownerKey", pattern.getOwnerKey()));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(eq("name", pattern.getName()));
        }
        Number storageNumberId = pattern.getStorageVolumeId();
        if (storageNumberId != null) {
            filtersBuilder.add(eq("storageVolumeId", storageNumberId));
        }
        Bson bsonFilter = null;
        List<Bson> filters = filtersBuilder.build();

        if (!filters.isEmpty()) bsonFilter = and(filters);

        return bsonFilter;
    }

    private List<Bson> getAggregationOps(JacsBundle pattern) {
        ImmutableList.Builder<Bson> bundleAggregationOpsBuilder = ImmutableList.builder();
        pattern.getStorageVolume().ifPresent(sv -> {
            bundleAggregationOpsBuilder.add(Aggregates.lookup(
                    EntityUtils.getMongoMapping(JacsStorageVolume.class).storeName(),
                    "storageVolumeId",
                    "_id",
                    "referencedVolumes"
            ));
            if (sv.isShared()) {
                bundleAggregationOpsBuilder.add(Aggregates.match(
                        Filters.or(
                                Filters.exists("referencedVolumes.storageHost", false),
                                Filters.eq("referencedVolumes.storageHost", null))
                ));
            } else if (sv.getStorageHost() != null) {
                if (StringUtils.isBlank(sv.getStorageHost())) {
                    bundleAggregationOpsBuilder.add(Aggregates.match(Filters.exists("referencedVolumes.storageHost", true)));
                    bundleAggregationOpsBuilder.add(Aggregates.match(Filters.ne("referencedVolumes.storageHost", null)));
                } else {
                    bundleAggregationOpsBuilder.add(Aggregates.match(Filters.eq("referencedVolumes.storageHost", sv.getStorageHost())));
                }
            }
            if (StringUtils.isNotBlank(sv.getName())) {
                bundleAggregationOpsBuilder.add(Aggregates.match(eq("referencedVolumes.name", sv.getName())));
            }
            if (StringUtils.isNotBlank(sv.getStoragePathPrefix())) {
                bundleAggregationOpsBuilder.add(Aggregates.match(eq("referencedVolumes.storagePathPrefix", sv.getStoragePathPrefix())));
            }
        });
        return bundleAggregationOpsBuilder.build();
    }
}
