package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

import java.util.Optional;

public interface StorageLookupService {
    JacsBundle getDataBundleById(Number id);
    PageResult<JacsBundle> findMatchingDataBundles(JacsBundle pattern, PageRequest pageRequest);
    JacsBundle findDataBundleByOwnerAndName(String owner, String name);
}
