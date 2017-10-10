package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

import java.util.Date;

public interface JacsBundleDao extends ReadWriteDao<JacsBundle> {
    JacsBundle findByOwnerAndName(String owner, String name);
    PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest);
}
