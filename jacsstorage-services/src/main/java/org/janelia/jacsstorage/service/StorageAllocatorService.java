package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;

import java.util.Optional;

public interface StorageAllocatorService {
    Optional<JacsBundle> allocateStorage(String dataBundlePathPrefix, JacsBundle dataBundle, JacsCredentials credentials);
    JacsBundle updateStorage(JacsBundle dataBundle, JacsCredentials credentials);
    boolean deleteStorage(JacsCredentials credentials, JacsBundle dataBundle);
    Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle);
}
