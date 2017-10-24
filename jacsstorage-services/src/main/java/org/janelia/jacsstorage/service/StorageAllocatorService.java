package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.Optional;

public interface StorageAllocatorService {
    Optional<JacsBundle> allocateStorage(JacsBundle dataBundle);
    JacsBundle updateStorage(JacsBundle dataBundle);
    boolean deleteStorage(JacsBundle dataBundle);
    Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle);
}
