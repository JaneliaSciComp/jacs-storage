package org.janelia.jacsstorage.service.distributedservice;

import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;

@RemoteInstance
public class DistributedStorageVolumeManager extends AbstractStorageVolumeManager {

    private final DistributedStorageHelper storageHelper;

    @Inject
    public DistributedStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                           StorageAgentManager agentManager,
                                           NotificationService capacityNotifier) {
        super(storageVolumeDao, capacityNotifier);
        this.storageHelper = new DistributedStorageHelper(agentManager);
    }

    @TimedMethod
    @Override
    public Optional<JacsStorageVolume> getFullVolumeInfo(String volumeName) {
        StorageQuery storageQuery = new StorageQuery().setStorageName(volumeName);
        return getManagedVolumes(storageQuery).stream().findFirst();
    }

    @TimedMethod
    @Override
    public List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery) {
        PageRequest pageRequest = new PageRequest();
        List<JacsStorageVolume> managedVolumes = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
        managedVolumes.forEach(storageHelper::updateStorageServiceInfo);
        return managedVolumes;
    }

}
