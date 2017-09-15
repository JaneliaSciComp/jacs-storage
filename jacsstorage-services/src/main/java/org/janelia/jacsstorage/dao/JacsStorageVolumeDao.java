package org.janelia.jacsstorage.dao;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;

public interface JacsStorageVolumeDao extends ReadWriteDao<JacsStorageVolume> {
    JacsStorageVolume findOrCreateByLocation(String location);
}
