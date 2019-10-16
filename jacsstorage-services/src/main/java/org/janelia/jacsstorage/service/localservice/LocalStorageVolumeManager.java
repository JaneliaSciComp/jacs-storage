package org.janelia.jacsstorage.service.localservice;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Function;
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
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
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
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        if (validateStorageVolume(storageVolume, canServeVolume(localStorageAgent).and(canAccessVolume()), Function.identity()) != null) {
            return super.createNewStorageVolume(storageVolume);
        } else {
            return null;
        }
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageAgentId) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        if (validateStorageVolume(new JacsStorageVolumeBuilder()
                .name(volumeName)
                .shared(StringUtils.isBlank(storageAgentId))
                .storageAgentId(storageAgentId)
                .build(),
                canServeVolume(localStorageAgent).and(canAccessVolume()),
                Function.identity()) != null) {
            return super.createStorageVolumeIfNotFound(volumeName, storageAgentId);
        } else {
            return null;
        }
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume getVolumeById(Number volumeId) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        return validateStorageVolume(super.getVolumeById(volumeId),
                canServeVolume(localStorageAgent).and(canAccessVolume()),
                sv -> fillAccessInfo(sv, localStorageAgent));
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
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        return validateStorageVolume(super.updateVolumeInfo(volumeId, storageVolume),
                canServeVolume(localStorageAgent).and(canAccessVolume()),
                sv -> fillAccessInfo(sv, localStorageAgent));
    }

    private JacsStorageVolume validateStorageVolume(JacsStorageVolume storageVolume,
                                                    Predicate<JacsStorageVolume> canServeTest,
                                                    Function<JacsStorageVolume, JacsStorageVolume> canServeAction) {
        if (storageVolume == null) {
            return null;
        } else {
            if (canServeTest.test(storageVolume)) {
                return canServeAction.apply(storageVolume);
            } else {
                return null;
            }
        }
    }

    private List<JacsStorageVolume> filterServedVolumes(List<JacsStorageVolume> storageVolumes) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        Predicate<JacsStorageVolume> canServeAndAccessVolumePredicate = canServeVolume(localStorageAgent).and(canAccessVolume());
        return storageVolumes.stream()
                .filter(canServeAndAccessVolumePredicate)
                .map(storageVolume -> fillAccessInfo(storageVolume, localStorageAgent))
                .collect(Collectors.toList());
    }

    private Predicate<JacsStorageVolume> canServeVolume(JacsStorageAgent localStorageAgent) {
        return storageVolume -> {
            if (storageVolume.isShared()) {
                return JacsStorageVolume.OVERFLOW_VOLUME.equals(storageVolume.getName()) ||
                        localStorageAgent.canServe(storageVolume.getName());
            } else {
                return localStorageAgent.getAgentHost().equals(storageVolume.getStorageAgentId()) &&
                        localStorageAgent.canServe(storageVolume.getName());
            }
        };
    }

    private Predicate<JacsStorageVolume> canAccessVolume() {
        return storageVolume -> Files.exists(Paths.get(storageVolume.getBaseStorageRootDir()));
    }

    private JacsStorageVolume fillAccessInfo(JacsStorageVolume storageVolume, JacsStorageAgent storageAgent) {
        storageVolume.setStorageServiceURL(storageAgent.getAgentAccessURL());
        return storageVolume;
    }
}
