package org.janelia.jacsstorage.dao.mongo;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

import org.apache.commons.lang3.StringUtils;
import org.bson.conversions.Bson;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsStorageAgentDao;
import org.janelia.jacsstorage.dao.JacsStorageEventDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageEvent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

/**
 * Mongo based implementation of JacsStorageEventDao.
 */
public class JacsStorageAgentMongoDao extends AbstractMongoDao<JacsStorageAgent> implements JacsStorageAgentDao {
    @Inject
    public JacsStorageAgentMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }

    @Override
    protected void createDocumentIndexes() {
    }

    @Override
    public JacsStorageAgent createStorageAgentIfNotFound(String agentHost, String agentAccessURL, String status, Set<String> servedVolumes) {
        Preconditions.checkArgument(StringUtils.isNotBlank(agentHost));

        FindOneAndUpdateOptions updateOptions = new FindOneAndUpdateOptions();
        updateOptions.returnDocument(ReturnDocument.AFTER);
        updateOptions.upsert(true);

        ImmutableList.Builder<Bson> filtersBuilder = new ImmutableList.Builder<>();
        filtersBuilder.add(Filters.eq("agentHost", agentHost));

        Bson fieldsToInsert = Updates.combine(
                Updates.setOnInsert("_id", idGenerator.generateId()),
                Updates.setOnInsert("agentHost", agentHost),
                Updates.setOnInsert("agentAccessURL", agentAccessURL),
                Updates.setOnInsert("servedVolumes", servedVolumes),
                Updates.setOnInsert("status", status),
                Updates.setOnInsert("lastStatusCheck", new Date()),
                Updates.setOnInsert("class", JacsStorageAgent.class.getName())
        );

        return mongoCollection.findOneAndUpdate(
                Filters.and(filtersBuilder.build()),
                fieldsToInsert,
                updateOptions
        );
    }

    @Override
    public JacsStorageAgent findStorageAgentByHost(String agentHost) {
        Iterator<JacsStorageAgent> resultsItr = findIterable(Filters.eq("agentHost", agentHost),
                null,
                0,
                0,
                getEntityType()).iterator();
        return resultsItr.hasNext() ? resultsItr.next() : null;
    }
}
