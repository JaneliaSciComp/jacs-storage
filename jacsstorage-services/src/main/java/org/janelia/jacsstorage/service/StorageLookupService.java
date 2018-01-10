package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

public interface StorageLookupService {
    JacsBundle getDataBundleById(Number id);
    JacsBundle findDataBundleByOwnerAndName(String owner, String name);
    long countMatchingDataBundles(JacsBundle pattern);
    PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest);
}
