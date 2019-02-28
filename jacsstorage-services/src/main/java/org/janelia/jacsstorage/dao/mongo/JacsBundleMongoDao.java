package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.apache.commons.collections4.CollectionUtils;
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

/**
 * Mongo based implementation of JacsBundleDao.
 */
public class JacsBundleMongoDao extends AbstractMongoDao<JacsBundle> implements JacsBundleDao {

    @Inject
    public JacsBundleMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
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
        filtersBuilder.add(Filters.eq("ownerKey", ownerKey));
        filtersBuilder.add(Filters.eq("name", name));

        Iterator<JacsBundle> resultsItr = findIterable(Filters.and(filtersBuilder.build()),
                null,
                0,
                0,
                getEntityType()).iterator();
        return resultsItr.hasNext() ? resultsItr.next() : null;
    }

    @Override
    public long countMatchingDataBundles(JacsBundle pattern) {
        return countAggregate(createMatchingPipeline(pattern));
    }

    @Override
    public PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest) {
        List<JacsBundle> results = aggregateAsList(
                createMatchingPipeline(pattern),
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                (int) pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());

        return new PageResult<>(pageRequest, results);
    }

    private Bson getBsonFilter(JacsBundle pattern) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", pattern.getId()));
        }
        if (StringUtils.isNotBlank(pattern.getOwnerKey())) {
            filtersBuilder.add(Filters.or(
                    Filters.eq("ownerKey", pattern.getOwnerKey()),
                    Filters.all("readersKeys", "*"), // everybody can read it
                    Filters.all("readersKeys", pattern.getOwnerKey())
            ));
        }
        if (StringUtils.isNotBlank(pattern.getName())) {
            filtersBuilder.add(Filters.eq("name", pattern.getName()));
        }
        Number storageNumberId = pattern.getStorageVolumeId();
        if (storageNumberId != null) {
            filtersBuilder.add(Filters.eq("storageVolumeId", storageNumberId));
        }
        Bson bsonFilter = null;
        List<Bson> filters = filtersBuilder.build();

        if (!filters.isEmpty()) bsonFilter = Filters.and(filters);

        return bsonFilter;
    }

    private List<Bson> createMatchingPipeline(JacsBundle pattern) {
        ImmutableList.Builder<Bson> pipelineBuilder = ImmutableList.builder();

        Bson matchFilter = getBsonFilter(pattern);
        if (matchFilter != null) {
            pipelineBuilder.add(Aggregates.match(matchFilter));
        }
        List<Bson> storageVolCriteria = pattern.getStorageVolume()
                .map(sv -> {
                    ImmutableList.Builder<Bson> localPipelineBuilder = ImmutableList.builder();
                    localPipelineBuilder.add(Aggregates.lookup(
                            EntityUtils.getPersistenceInfo(JacsStorageVolume.class).storeName(),
                            "storageVolumeId",
                            "_id",
                            "storageVolume"
                    ));
                    localPipelineBuilder.add(
                            Aggregates.unwind("$storageVolume")
                    );

                    if (sv.isShared()) {
                        localPipelineBuilder.add(Aggregates.match(Filters.or(
                                Filters.exists("storageVolume.storageHost", false),
                                Filters.eq("storageVolume.storageHost", null)))
                        );
                    } else if (sv.getStorageHost() != null) {
                        if (StringUtils.isBlank(sv.getStorageHost())) {
                            localPipelineBuilder.add(Aggregates.match(Filters.exists("storageVolume.storageHost", true)));
                            localPipelineBuilder.add(Aggregates.match(Filters.ne("storageVolume.storageHost", null)));
                        } else {
                            localPipelineBuilder.add(Aggregates.match(Filters.eq("storageVolume.storageHost", sv.getStorageHost())));
                        }
                    }
                    if (StringUtils.isNotBlank(sv.getName())) {
                        localPipelineBuilder.add(Aggregates.match(Filters.eq("storageVolume.name", sv.getName())));
                    }
                    if (CollectionUtils.isNotEmpty(sv.getStorageTags())) {
                        localPipelineBuilder.add(Aggregates.match(Filters.all("storageVolume.storageTags", sv.getStorageTags())));
                    }
                    if (StringUtils.isNotBlank(sv.getStorageVirtualPath())) {
                        localPipelineBuilder.add(Aggregates.match(Filters.eq("storageVolume.storageVirtualPath", sv.getStorageVirtualPath())));
                    }
                    if (StringUtils.isNotBlank(pattern.getPath())) {
                        localPipelineBuilder.add(
                                Aggregates.addFields(new Field<>(
                                        "dbId",
                                        createToLowerExpr("$_id")
                                ))
                        );
                        localPipelineBuilder.add(
                                Aggregates.addFields(new Field<>(
                                        "dataStoragePath",
                                        createConcatExpr("$storageVolume.storageVirtualPath", "/", "$dbId")
                                ))
                        );
                        localPipelineBuilder.add(Aggregates.match(Filters.eq("dataStoragePath", pattern.getPath())));
                    }
                    return localPipelineBuilder.build();
                })
                .orElseGet(() -> {
                    ImmutableList.Builder<Bson> localPipelineBuilder = ImmutableList.builder();

                    if (StringUtils.isNotBlank(pattern.getPath())) {
                        localPipelineBuilder.add(Aggregates.lookup(
                                EntityUtils.getPersistenceInfo(JacsStorageVolume.class).storeName(),
                                "storageVolumeId",
                                "_id",
                                "storageVolume"
                        ));
                        localPipelineBuilder.add(
                                Aggregates.unwind("$storageVolume")
                        );
                        localPipelineBuilder.add(
                                Aggregates.addFields(new Field<>(
                                        "dbId",
                                        createToLowerExpr("$_id")
                                ))
                        );
                        localPipelineBuilder.add(
                                Aggregates.addFields(new Field<>(
                                        "dataStoragePath",
                                        createConcatExpr("$storageVolume.storageVirtualPath", "/", "$dbId")
                                ))
                        );
                        localPipelineBuilder.add(Aggregates.match(Filters.eq("dataStoragePath", pattern.getPath())));
                    }
                    return localPipelineBuilder.build();
                });
        pipelineBuilder.addAll(storageVolCriteria);
        return pipelineBuilder.build();
    }
}
