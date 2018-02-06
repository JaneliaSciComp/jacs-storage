package org.janelia.jacsstorage.dao.mongo;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.SortCriteria;
import org.janelia.jacsstorage.datarequest.SortDirection;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Filters.eq;

/**
 * Mongo based implementation of JacsVolumeDao.
 */
public class JacsStorageVolumeMongoDao extends AbstractMongoDao<JacsStorageVolume> implements JacsStorageVolumeDao {
    @Inject
    public JacsStorageVolumeMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
        mongoCollection.createIndex(Indexes.ascending("storageHost", "name"),
                new IndexOptions()
                        .unique(true)
                        .sparse(true));
        mongoCollection.createIndex(Indexes.ascending("storagePathPrefix"),
                new IndexOptions().unique(true).sparse(true));
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
        return mongoCollection.count(Filters.and(createMatchingFilter(storageQuery)));
    }

    @Override
    public PageResult<JacsStorageVolume> findMatchingVolumes(StorageQuery storageQuery, PageRequest pageRequest) {
        List<JacsStorageVolume> results = new ArrayList<>();
        List<Bson> storageFilters = createMatchingFilter(storageQuery);
        Iterable<JacsStorageVolume> storageVolumesItr = findIterable(
                CollectionUtils.isNotEmpty(storageFilters)
                        ? Filters.and(storageFilters)
                        : null,
                createBsonSortCriteria(ImmutableList.of(new SortCriteria("storagePathPrefix", SortDirection.DESC)), pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        if (StringUtils.isBlank(storageQuery.getDataStoragePath())) {
            storageVolumesItr.forEach(results::add);
        } else {
            StreamSupport.stream(storageVolumesItr.spliterator(), false)
                    .filter(sv -> StringUtils.startsWith(storageQuery.getDataStoragePath(), sv.getStoragePathPrefix()) || StringUtils.startsWith(storageQuery.getDataStoragePath(), sv.getStorageRootDir()))
                    .forEach(results::add);
        }
        return new PageResult<>(pageRequest, results);
    }

    private List<Bson> createMatchingFilter(StorageQuery storageQuery) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (storageQuery.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", storageQuery.getId()));
        }
        if (storageQuery.isLocalToAnyHost() || CollectionUtils.isNotEmpty(storageQuery.getStorageHosts())) {
            if (CollectionUtils.isNotEmpty(storageQuery.getStorageHosts())) {
                filtersBuilder.add(Filters.in("storageHost", storageQuery.getStorageHosts()));
            } else {
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
        if (StringUtils.isNotBlank(storageQuery.getStoragePathPrefix())) {
            filtersBuilder.add(Filters.eq("storagePathPrefix", storageQuery.getStoragePathPrefix()));
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
        return filtersBuilder.build();
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
