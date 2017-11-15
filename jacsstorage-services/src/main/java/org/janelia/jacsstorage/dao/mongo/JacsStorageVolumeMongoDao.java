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
import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.DataInterval;
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
    }

    @Override
    public Long countMatchingVolumes(JacsStorageVolume pattern) {
        return mongoCollection.count(Filters.and(createMatchingFilter(pattern)));
    }

    private List<Bson> createMatchingFilter(JacsStorageVolume pattern) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        if (pattern.getId() != null) {
            filtersBuilder.add(Filters.eq("_id", pattern.getId()));
        }
        if (pattern.isShared()) {
            filtersBuilder.add(Filters.or(
                    Filters.exists("storageHost", false),
                    Filters.eq("storageHost", null)
            )); // the storageHost must not be set
        } else if (pattern.getStorageHost() != null) {
            if (StringUtils.isBlank(pattern.getStorageHost())) {
                filtersBuilder.add(Filters.exists("storageHost", true)); // the storageHost must be set
                filtersBuilder.add(Filters.ne("storageHost", null)); // the storageHost must be set
            } else {
                filtersBuilder.add(Filters.eq("storageHost", pattern.getStorageHost()));
            }
        }
        if (pattern.getName() != null) {
            filtersBuilder.add(Filters.eq("name", pattern.getName()));
        }
        if (pattern.hasTags()) {
            filtersBuilder.add(Filters.all("volumeTags", pattern.getStorageTags()));
        }
        if (pattern.hasAvailableSpaceInBytes()) {
            filtersBuilder.add(Filters.gte("availableSpaceInBytes", pattern.getAvailableSpaceInBytes()));
        }
        return filtersBuilder.build();
    }

    @Override
    public PageResult<JacsStorageVolume> findMatchingVolumes(JacsStorageVolume pattern, PageRequest pageRequest) {
        List<JacsStorageVolume> results = find(Filters.and(createMatchingFilter(pattern)),
                createBsonSortCriteria(pageRequest.getSortCriteria()),
                pageRequest.getOffset(),
                pageRequest.getPageSize(),
                getEntityType());
        return new PageResult<>(pageRequest, results);
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
