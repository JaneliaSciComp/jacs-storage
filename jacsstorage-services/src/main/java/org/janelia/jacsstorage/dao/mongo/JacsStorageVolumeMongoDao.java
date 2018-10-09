package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.SortCriteria;
import org.janelia.jacsstorage.datarequest.SortDirection;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Mongo based implementation of JacsStorageVolumeDao.
 */
public class JacsStorageVolumeMongoDao extends AbstractMongoDao<JacsStorageVolume> implements JacsStorageVolumeDao {
    @Inject
    public JacsStorageVolumeMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
        mongoCollection.createIndex(Indexes.ascending("storageHost", "name"),
                new IndexOptions()
                        .unique(true)
                        .sparse(true));
        mongoCollection.createIndex(Indexes.ascending("storagePathPrefix"),
                new IndexOptions().sparse(true));
        mongoCollection.createIndex(Indexes.ascending("storageServiceURL"));
    }

    @Override
    public void update(JacsStorageVolume entity, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        Map<String, EntityFieldValueHandler<?>> fieldsWithUpdatedDate = new LinkedHashMap<>(fieldsToUpdate);
        entity.setModified(new Date());
        fieldsWithUpdatedDate.put("modified", new SetFieldValueHandler<>(entity.getModified()));
        super.update(entity, fieldsWithUpdatedDate);
    }

    @Override
    public Long countMatchingVolumes(StorageQuery storageQuery) {
        return countAggregate(createMatchingPipeline(storageQuery));
    }

    @Override
    public PageResult<JacsStorageVolume> findMatchingVolumes(StorageQuery storageQuery, PageRequest pageRequest) {
        List<JacsStorageVolume> results = new ArrayList<>();
        Iterable<JacsStorageVolume> storageVolumesItr = aggregateIterable(
                createMatchingPipeline(storageQuery),
                createBsonSortCriteria(ImmutableList.of(
                        new SortCriteria("storageVirtualPath", SortDirection.DESC)),
                        pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType()
        );
        if (StringUtils.isBlank(storageQuery.getDataStoragePath())) {
            storageVolumesItr.forEach(results::add);
        } else {
            StreamSupport.stream(storageVolumesItr.spliterator(), false)
                    .filter(sv -> ExprHelper.match(sv.getStorageVirtualPath(), storageQuery.getDataStoragePath()).isMatchFound() ||
                                    ExprHelper.match(sv.getStorageRootTemplate(), storageQuery.getDataStoragePath()).isMatchFound()
                            )
                    .forEach(results::add);
        }
        return new PageResult<>(pageRequest, results);
    }

    private List<Bson> createMatchingPipeline(StorageQuery storageQuery) {
        ImmutableList.Builder<Bson> pipelineBuilder = new ImmutableList.Builder<>();
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (storageQuery.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", storageQuery.getId()));
        }
        if (StringUtils.isNotBlank(storageQuery.getAccessibleOnHost())) {
            if (storageQuery.isLocalToAnyHost()) {
                filtersBuilder.add(Filters.eq("storageHost", storageQuery.getAccessibleOnHost())); // select volumes accessible ONLY from the specified host
            } else {
                filtersBuilder.add(Filters.or(
                        Filters.eq("storageHost", storageQuery.getAccessibleOnHost()), // the storage host equals the one set
                        Filters.exists("storageHost", false), // or the storage host is not set
                        Filters.eq("storageHost", null)
                ));
            }
        } else if (storageQuery.isLocalToAnyHost() || CollectionUtils.isNotEmpty(storageQuery.getStorageHosts())) {
            if (CollectionUtils.isNotEmpty(storageQuery.getStorageHosts())) {
                // this queries for volumes accessible only locally on the specified hosts
                filtersBuilder.add(Filters.in("storageHost", storageQuery.getStorageHosts()));
            } else {
                // this queries for volumes accessible only locally on the corresponding hosts
                filtersBuilder.add(Filters.exists("storageHost", true)); // the storageHost must be set
                filtersBuilder.add(Filters.ne("storageHost", null));
            }
        } else if (storageQuery.isShared()) {
            filtersBuilder.add(Filters.or(
                    Filters.exists("storageHost", false), // the storage host should not be set
                    Filters.eq("storageHost", null)
            )); // the storageHost must not be set
        }
        if (StringUtils.isNotBlank(storageQuery.getStorageName())) {
            filtersBuilder.add(Filters.eq("name", storageQuery.getStorageName()));
        }
        if (StringUtils.isNotBlank(storageQuery.getDataStoragePath())) {
            // the way this filter works is by checking if the current storage root directory (or virtual path)
            // are a prefix of the given argument.
            // If the condition is met then a field with the same value is added to the projection
            // and the field is matched against the query
            Bson storageRootBase = ifNullExp(createStorageRootBaseExpr(), literalExp("--"));
            pipelineBuilder.add(Aggregates.addFields(
                    new Field<>("storageRootBase",
                            createStartsWithExpr(storageQuery.getDataStoragePath(), storageRootBase)
                    ),
                    new Field<>(
                            "dataStoragePath",
                            createCondExpr(
                                    createStartsWithExpr(storageQuery.getDataStoragePath(), storageRootBase),
                                    storageQuery.getDataStoragePath(),
                                    createCondExpr(
                                            createStartsWithExpr(storageQuery.getDataStoragePath(), "$storageVirtualPath"),
                                            storageQuery.getDataStoragePath(),
                                            ""
                                    )
                            )
                    )
            ));
            filtersBuilder.add(Filters.eq("dataStoragePath", storageQuery.getDataStoragePath()));
        }
        if (StringUtils.isNotBlank(storageQuery.getStorageVirtualPath())) {
            filtersBuilder.add(Filters.eq("storageVirtualPath", storageQuery.getStorageVirtualPath()));
        }
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageAgents())) {
            if (storageQuery.isShared()) {
                filtersBuilder.add(Filters.or(
                        Filters.exists("storageServiceURL", false), // the storage host should not be set
                        Filters.eq("storageServiceURL", null)
                )); // the storageServiceURL must not be set
            } else if (storageQuery.isLocalToAnyHost()) {
                filtersBuilder.add(Filters.in("storageServiceURL", storageQuery.getStorageAgents()));
            }
        }
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageTags())) {
            filtersBuilder.add(Filters.all("storageTags", storageQuery.getStorageTags()));
        }
        if (storageQuery.hasMinAvailableSpaceInBytes()) {
            filtersBuilder.add(Filters.gte("availableSpaceInBytes", storageQuery.getMinAvailableSpaceInBytes()));
        }
        if (!storageQuery.isIncludeInactiveVolumes()) {
            filtersBuilder.add(Filters.eq("activeFlag", true));
        }
        pipelineBuilder.add(Aggregates.match(Filters.and(filtersBuilder.build())));
        return pipelineBuilder.build();
    }

    private Bson createStorageRootBaseExpr() {
        Bson indexOfVarExpr = new Document("$indexOfCP", Arrays.asList("$storageRootTemplate", literalExp("$")));
        Bson indexOfVarWithDefaultExpr = ifNullExp(indexOfVarExpr, -1);
        return createCondExpr(createEqExpr(indexOfVarWithDefaultExpr, -1),
                "$storageRootTemplate",
                createSubstrExpr("$storageRootTemplate",  0, indexOfVarExpr));
    }

    private Bson literalExp(Object exp) {
        return new Document("$literal", exp);
    }

    private Bson ifNullExp(Object expr, Object nullDefault) {
        return new Document("$ifNull", Arrays.asList(expr, nullDefault));
    }

    private Bson createCondExpr(Object cond, Object thenValue, Object elseValue) {
        return new Document("$cond",
                Arrays.asList(
                        cond,
                        thenValue,
                        elseValue
                ));
    }

    private Bson createStartsWithExpr(Object expr, Object subExpr) {
        return createEqExpr(createIndexOfExpr(expr, subExpr), 0);
    }

    private Bson createEqExpr(Object ...argList) {
        return new Document("$eq", Arrays.asList(argList));
    }

    private Bson createIndexOfExpr(Object expr, Object subExpr) {
        return new Document("$indexOfBytes", Arrays.asList(expr, subExpr));
    }

    private Bson createSubstrExpr(Object strExpr, Object startIndexExpr, Object countExpr) {
        return new Document("$substrBytes", Arrays.asList(strExpr, startIndexExpr, countExpr));
    }

    @Override
    public JacsStorageVolume getStorageByHostAndNameAndCreateIfNotFound(String hostName, String volumeName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(volumeName));

        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);
        updateOptions.upsert(true);

        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (StringUtils.isBlank(hostName)) {
            filtersBuilder.add(Filters.or(Filters.exists("storageHost", false), Filters.eq("storageHost", null)));
        } else {
            filtersBuilder.add(Filters.eq("storageHost", hostName));
        }
        filtersBuilder.add(Filters.eq("name", volumeName));

        Bson fieldsToInsert = Updates.combine(
                Updates.setOnInsert("_id", idGenerator.generateId()),
                Updates.setOnInsert("storageHost", StringUtils.defaultIfBlank(hostName, null)),
                Updates.setOnInsert("name", volumeName),
                Updates.setOnInsert("shared", StringUtils.isBlank(hostName)),
                Updates.setOnInsert("created", new Date()),
                Updates.setOnInsert("class", JacsStorageVolume.class.getName())
        );
        return mongoCollection.findOneAndUpdate(
                Filters.and(filtersBuilder.build()),
                fieldsToInsert,
                updateOptions
        );
    }
}
