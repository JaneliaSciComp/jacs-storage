package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

import java.util.List;

public interface StorageVolumeSelector {
    JacsStorageVolume selectStorageVolume(JacsBundle storageRequest);
}
