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

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume) {
        JacsStorageVolume newStorageVolume = super.createNewStorageVolume(storageVolume);
        storageHelper.updateStorageServiceInfo(newStorageVolume);
        return newStorageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageAgentId) {
        JacsStorageVolume newStorageVolume = super.createStorageVolumeIfNotFound(volumeName, storageAgentId);
        storageHelper.updateStorageServiceInfo(newStorageVolume);
        return newStorageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume getVolumeById(Number volumeId) {
        JacsStorageVolume storageVolume = super.getVolumeById(volumeId);
        storageHelper.updateStorageServiceInfo(storageVolume);
        return storageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public List<JacsStorageVolume> findVolumes(StorageQuery storageQuery) {
        PageRequest pageRequest = new PageRequest();
        List<JacsStorageVolume> managedVolumes = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
        managedVolumes.forEach(storageHelper::updateStorageServiceInfo);
        return managedVolumes;
    }

    @TimedMethod(
            logLevel = "trace"
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        JacsStorageVolume updatedStorageVolume = super.updateVolumeInfo(volumeId, storageVolume);
        storageHelper.updateStorageServiceInfo(updatedStorageVolume);
        return updatedStorageVolume;
    }
}
