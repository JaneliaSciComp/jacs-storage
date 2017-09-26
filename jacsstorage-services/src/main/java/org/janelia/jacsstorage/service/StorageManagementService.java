package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;

import java.util.Optional;

public interface StorageManagementService {
    Optional<JacsBundle> allocateStorage(JacsBundle dataBundle);
    JacsBundle getDataBundleById(Number id);
    JacsBundle findDataBundleByOwnerAndName(String owner, String name);
    JacsBundle updateDataBundle(JacsBundle dataBundle);
}
