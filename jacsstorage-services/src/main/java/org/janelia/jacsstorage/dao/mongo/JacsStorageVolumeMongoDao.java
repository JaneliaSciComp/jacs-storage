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
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import javax.inject.Inject;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;

/**
 * Mongo based implementation of JacsVolumeDao.
 */
public class JacsStorageVolumeMongoDao extends AbstractMongoDao<JacsStorageVolume> implements JacsStorageVolumeDao {
    @Inject
    public JacsStorageVolumeMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
        IndexOptions indexOptions = new IndexOptions()
                .unique(true)
                .sparse(true);
        mongoCollection.createIndex(Indexes.ascending("storageHost", "name"), indexOptions);
        mongoCollection.createIndex(Indexes.ascending("storageServiceURL"));
    }

    @Override
    public Long countMatchingVolumes(StorageQuery storageQuery) {
        return mongoCollection.count(Filters.and(createMatchingFilter(storageQuery)));
    }

    @Override
    public PageResult<JacsStorageVolume> findMatchingVolumes(StorageQuery storageQuery, PageRequest pageRequest) {
        List<JacsStorageVolume> results = find(Filters.and(createMatchingFilter(storageQuery)),
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        return new PageResult<>(pageRequest, results);
    }

    private List<Bson> createMatchingFilter(StorageQuery storageQuery) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (storageQuery.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", storageQuery.getId()));
        }
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageHosts())) {
            filtersBuilder.add(Filters.in("storageHost", storageQuery.getStorageHosts()));
        } else if (storageQuery.isShared()) {
            filtersBuilder.add(Filters.or(
                    Filters.exists("storageHost", false), // the storage host should not be set
                    Filters.eq("storageHost", null)
            )); // the storageHost must not be set
        } else if (storageQuery.isLocalToAnyHost()) {
            filtersBuilder.add(Filters.exists("storageHost", true)); // the storageHost must be set
            filtersBuilder.add(Filters.ne("storageHost", null));
        }
        if (StringUtils.isNotBlank(storageQuery.getStorageName())) {
            filtersBuilder.add(Filters.eq("name", storageQuery.getStorageName()));
        }
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageAgents())) {
            filtersBuilder.add(Filters.in("storageServiceURL", storageQuery.getStorageAgents()));
        }
        if (CollectionUtils.isNotEmpty(storageQuery.getStorageTags())) {
            filtersBuilder.add(Filters.all("volumeTags", storageQuery.getStorageTags()));
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
