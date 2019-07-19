package org.janelia.jacsstorage.service.localservice;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LocalInstance
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageVolumeManager.class);

    private final AgentStatePersistence storageAgentPersistence;

    @Inject
    public LocalStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                     AgentStatePersistence storageAgentPersistence,
                                     NotificationService capacityNotifier) {
        super(storageVolumeDao, capacityNotifier);
        this.storageAgentPersistence = storageAgentPersistence;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume) {
        return validateStorageVolume(super.createNewStorageVolume(storageVolume));
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageHost) {
        return validateStorageVolume(super.createStorageVolumeIfNotFound(volumeName, storageHost));
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume getVolumeById(Number volumeId) {
        return validateStorageVolume(super.getVolumeById(volumeId));
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public List<JacsStorageVolume> findVolumes(StorageQuery storageQuery) {
        LOG.trace("Query managed volumes using {}", storageQuery);
        PageRequest pageRequest = new PageRequest();
        return filterServedVolumes(storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList());
    }

    @TimedMethod(
            logLevel = "trace"
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        return validateStorageVolume(super.updateVolumeInfo(volumeId, storageVolume));
    }

    private JacsStorageVolume validateStorageVolume(JacsStorageVolume storageVolume) {
        if (storageVolume == null) {
            return null;
        } else {
            JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
            if (canServeVolume(localStorageAgent).test(storageVolume)) {
                return fillAccessInfo(storageVolume, localStorageAgent);
            } else {
                return null;
            }
        }
    }

    private List<JacsStorageVolume> filterServedVolumes(List<JacsStorageVolume> storageVolumes) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        Predicate<JacsStorageVolume> canServeVolumePredicate = canServeVolume(localStorageAgent);
        return storageVolumes.stream()
                .filter(canServeVolumePredicate)
                .map(storageVolume -> fillAccessInfo(storageVolume, localStorageAgent))
                .collect(Collectors.toList());
    }

    private Predicate<JacsStorageVolume> canServeVolume(JacsStorageAgent localStorageAgent) {
        return storageVolume -> {
            if (StringUtils.isNotBlank(storageVolume.getStorageHost())) {
                return storageVolume.getStorageHost().equals(localStorageAgent.getAgentHost());
            } else {
                return localStorageAgent.canServe(storageVolume.getName());
            }
        };
    }

    private JacsStorageVolume fillAccessInfo(JacsStorageVolume storageVolume, JacsStorageAgent storageAgent) {
        storageVolume.setStorageServiceURL(storageAgent.getAgentAccessURL());
        return storageVolume;
    }
}
