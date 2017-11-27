package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.AbstractStorageVolumeManager;

import javax.inject.Inject;
import java.util.List;

@RemoteInstance
public class DistributedStorageVolumeManager extends AbstractStorageVolumeManager {

    @Inject
    public DistributedStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao) {
        super(storageVolumeDao);
    }

    public List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery) {
        PageRequest pageRequest = new PageRequest();
        return storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
    }

}
