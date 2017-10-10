package org.janelia.jacsstorage.dao.mongo;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import javax.inject.Inject;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Mongo based implementation of JacsVolumeDao.
 */
public class JacsStorageVolumeMongoDao extends AbstractMongoDao<JacsStorageVolume> implements JacsStorageVolumeDao {
    @Inject
    public JacsStorageVolumeMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
        IndexOptions indexOptions = new IndexOptions();
        indexOptions.unique(true);
        mongoCollection.createIndex(Indexes.ascending("location"), indexOptions);
    }

    @Override
    public Optional<JacsStorageVolume> findStorageByLocation(String location) {
        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(Filters.eq("location", location));

        List<JacsStorageVolume> results = find(Filters.and(filtersBuilder.build()),
                null,
                0,
                0,
                getEntityType());
        return results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
    }

    @Override
    public JacsStorageVolume getStorageByLocationAndCreateIfNotFound(String location) {
        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);
        updateOptions.upsert(true);

        Bson fieldsToInsert = Updates.combine(
                Updates.setOnInsert("_id", idGenerator.generateId()),
                Updates.setOnInsert("location", location),
                Updates.setOnInsert("created", new Date()),
                Updates.setOnInsert("class", JacsStorageVolume.class.getName())
        );
        return mongoCollection.findOneAndUpdate(
                Filters.eq("location", location),
                fieldsToInsert,
                updateOptions
        );
    }
}
