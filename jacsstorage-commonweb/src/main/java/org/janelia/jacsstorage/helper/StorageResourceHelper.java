package org.janelia.jacsstorage.helper;

import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timed
public class StorageResourceHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StorageResourceHelper.class);

    private final StorageVolumeManager storageVolumeManager;

    public StorageResourceHelper(StorageVolumeManager storageVolumeManager) {
        this.storageVolumeManager = storageVolumeManager;
    }

    /**
     * List storage volumes that match the input URI. If the input URI is empty it throws IllegalArgumentException.
     *
     * @param storageURI
     * @return
     */
    public List<JacsStorageVolume> listStorageVolumesForURI(JADEStorageURI storageURI) {
        if (storageURI.isEmpty()) {
            throw new IllegalArgumentException("Root storage location is not accepted: " + storageURI);
        }
        try {
            return retrieveStorageVolumesForDataPath(storageURI.getJadeStorage());
        } catch (Exception e) {
            LOG.error("Error retrieving any volume for {}", storageURI, e);
            return Collections.emptyList();
        }
    }

    private List<JacsStorageVolume> retrieveStorageVolumesForDataPath(String dataPath) {
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(new StorageQuery().setDataStoragePath(dataPath));
        if (storageVolumes.isEmpty()) {
            LOG.warn("No volume found to match {}", dataPath);
        } else {
            // Based on a first look the directory may be accessible from multiple volumes
            // but later the volumes will be narrowed down even further using the entire data path
            // E.g. we have 2 non-shared volumes served by node1 and node2 and both use the same root directory /data/jacsstorage
            // Now if one searches for a directory /data/jacsstorage/dirOnNode2Only, which is only available on node1
            // the master may redirect the caller to the wrong agent - node1 instead of node2 because at this point it has no information
            // whether the directory really exists
            LOG.debug("Storage volumes found for {} -> {}", dataPath, storageVolumes);
        }
        return storageVolumes;
    }

}
