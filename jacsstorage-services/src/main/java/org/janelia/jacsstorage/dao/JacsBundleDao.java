package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

public interface JacsBundleDao extends ReadWriteDao<JacsBundle> {
    JacsBundle findByNameAndOwner(String owner, String name);
}
