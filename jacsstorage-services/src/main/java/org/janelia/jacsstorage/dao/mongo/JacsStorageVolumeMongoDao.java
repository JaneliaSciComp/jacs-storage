package org.janelia.jacsstorage.dao.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
        mongoCollection.createIndex(Indexes.ascending("storageAgentId", "name"),
                new IndexOptions()
                        .unique(true)
                        .sparse(true));
        mongoCollection.createIndex(Indexes.ascending("storagePathPrefix"),
                new IndexOptions().sparse(true));
        mongoCollection.createIndex(Indexes.ascending("storageServiceURL"));
    }

    @Override
    public JacsStorageVolume update(Number entityId, Map<String, EntityFieldValueHandler<?>> fieldsToUpdate) {
        return super.update(
                entityId,
                fieldsToUpdate.containsKey("modified")
                        ? fieldsToUpdate
                        : ImmutableMap.<String, EntityFieldValueHandler<?>>builder().putAll(fieldsToUpdate).put("modified", new SetFieldValueHandler<>(new Date()))
                        .build()
        );
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
        if (StringUtils.isNotBlank(storageQuery.getAccessibleOnAgent())) {
            if (storageQuery.isLocalToAnyAgent()) {
                filtersBuilder.add(Filters.eq("storageAgentId", storageQuery.getAccessibleOnAgent())); // select volumes accessible ONLY from the specified host
            } else {
                filtersBuilder.add(Filters.or(
                        Filters.eq("storageAgentId", storageQuery.getAccessibleOnAgent()), // the storage host equals the one set
                        Filters.exists("storageAgentId", false), // or the storage host is not set
                        Filters.eq("storageAgentId", null)
                ));
            }
        } else if (storageQuery.isLocalToAnyAgent() || CollectionUtils.isNotEmpty(storageQuery.getStorageAgentIds())) {
            if (CollectionUtils.isNotEmpty(storageQuery.getStorageAgentIds())) {
                // this queries for volumes accessible only locally on the specified hosts
                filtersBuilder.add(Filters.in("storageAgentId", storageQuery.getStorageAgentIds()));
            } else {
                // this queries for volumes accessible only locally on the corresponding hosts
                filtersBuilder.add(Filters.exists("storageAgentId", true)); // the storageAgentId must be set
                filtersBuilder.add(Filters.ne("storageAgentId", null));
            }
        } else if (storageQuery.isShared()) {
            filtersBuilder.add(Filters.or(
                    Filters.exists("storageAgentId", false), // the storage agent should not be set
                    Filters.eq("storageAgentId", null),
                    Filters.eq("shared", true)
            )); // the storageAgentId must not be set
        }
        if (StringUtils.isNotBlank(storageQuery.getStorageName())) {
            filtersBuilder.add(Filters.eq("name", storageQuery.getStorageName()));
        }
        if (StringUtils.isNotBlank(storageQuery.getDataStoragePath())) {
            // the way this filter works is by checking if the current storage root directory (or virtual path)
            // are a prefix of the given argument.
            // If the condition is met then a field with the same value is added to the projection
            // and the field is matched against the query
            Bson storageRootBase = createStorageRootBaseExpr();
            pipelineBuilder.add(Aggregates.addFields(
                    new Field<>("storageRootBase",
                            createStartsWithExpr(storageQuery.getDataStoragePath(), ifNullExp(storageRootBase, literalExp("$$$")))
                    ),
                    new Field<>(
                            "dataStoragePath",
                            createCondExpr(
                                    createStartsWithExpr(storageQuery.getDataStoragePath(), ifNullExp(storageRootBase, literalExp("$$$"))),
                                    storageQuery.getDataStoragePath(),
                                    createCondExpr(
                                            createStartsWithExpr(storageQuery.getDataStoragePath(), ifNullExp("$storageVirtualPath", literalExp("$$$"))),
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
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageAgentURLs())) {
            if (storageQuery.isShared()) {
                filtersBuilder.add(Filters.or(
                        Filters.exists("storageServiceURL", false), // the storage host should not be set
                        Filters.eq("storageServiceURL", null)
                )); // the storageServiceURL must not be set
            } else if (storageQuery.isLocalToAnyAgent()) {
                filtersBuilder.add(Filters.in("storageServiceURL", storageQuery.getStorageAgentURLs()));
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
                createSubstrExpr("$storageRootTemplate", 0, indexOfVarExpr));
    }

    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String agentId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(volumeName));

        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);
        updateOptions.upsert(true);

        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        String storageAgentId;
        boolean sharedVolume;
        if (StringUtils.isBlank(agentId)) {
            sharedVolume = true;
            storageAgentId = null;
            filtersBuilder.add(Filters.or(Filters.exists("storageAgentId", false), Filters.eq("storageAgentId", null)));
        } else {
            sharedVolume = false;
            storageAgentId = agentId;
            filtersBuilder.add(Filters.eq("storageAgentId", agentId));
        }
        filtersBuilder.add(Filters.eq("name", volumeName));

        Date changedTimestamp = new Date();
        Bson fieldsToInsert = Updates.combine(
                Updates.setOnInsert("_id", idGenerator.generateId()),
                Updates.setOnInsert("name", volumeName),
                Updates.setOnInsert("storageAgentId", storageAgentId),
                Updates.setOnInsert("shared", sharedVolume),
                Updates.setOnInsert("created", changedTimestamp),
                Updates.set("modified", changedTimestamp),
                Updates.setOnInsert("class", JacsStorageVolume.class.getName())
        );
        return mongoCollection.findOneAndUpdate(
                Filters.and(filtersBuilder.build()),
                fieldsToInsert,
                updateOptions
        );
    }
}
