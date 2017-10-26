package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;

import java.util.Optional;

public interface StorageAllocatorService {
    Optional<JacsBundle> allocateStorage(JacsCredentials credentials, JacsBundle dataBundle);
    JacsBundle updateStorage(JacsCredentials credentials, JacsBundle dataBundle);
    boolean deleteStorage(JacsCredentials credentials, JacsBundle dataBundle);
    Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle);
}
