package org.janelia.jacsstorage.dao.mongo;

import com.mongodb.client.MongoDatabase;
import org.janelia.jacsstorage.dao.IdGenerator;
import org.janelia.jacsstorage.dao.JacsVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsVolume;

import javax.inject.Inject;

/**
 * Mongo based implementation of JacsVolumeDao.
 */
public class JacsVolumeMongoDao extends AbstractMongoDao<JacsVolume> implements JacsVolumeDao {
    @Inject
    public JacsVolumeMongoDao(MongoDatabase mongoDatabase, IdGenerator idGenerator) {
        super(mongoDatabase, idGenerator);
    }
}
