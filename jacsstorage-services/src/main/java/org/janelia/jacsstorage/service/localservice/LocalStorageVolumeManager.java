package org.janelia.jacsstorage.service.localservice;

import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LocalInstance
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageVolumeManager.class);
    private static final JacsStorageAgent NO_STORAGE_AGENT = new JacsStorageAgent();

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
        if (canServeVolume(localStorageAgent).and(canAccessVolume()).test(storageVolume)) {
            return super.createNewStorageVolume(storageVolume);
        } else {
            LOG.warn("Didn't create new storage volume for {} because the volume either cannot be served or is not accessible by the local agent {}", storageVolume, localStorageAgent);
            return null;
        }
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, String storageAgentId) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        if (canServeVolume(localStorageAgent).test(new JacsStorageVolumeBuilder()
                .name(volumeName)
                .shared(StringUtils.isBlank(storageAgentId))
                .storageAgentId(storageAgentId)
                .build())) {
            // only create the volume if it can be served by this agent instance
            return super.createStorageVolumeIfNotFound(volumeName, storageAgentId);
        } else {
            LOG.info("Did not try to check or create a new volume {} because it cannot be served by local agent {}", volumeName, localStorageAgent);
            return null;
        }
    }

    @TimedMethod(
            logLevel = "debug",
            logResult = true
    )
    @Override
    public JacsStorageVolume getVolumeById(Number volumeId) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        JacsStorageVolume storageVolume = storageVolumeDao.findById(volumeId);
        if (storageVolume == null) {
            LOG.error("No volume found for {}", volumeId);
            return null;
        } else {
            if (canServeVolume(localStorageAgent).test(storageVolume)) {
                if (canAccessVolume().test(storageVolume)) {
                    fillAccessInfo(storageVolume, localStorageAgent);
                    return storageVolume;
                } else {
                    LOG.warn("Volume {} is not accessible by agent {}", storageVolume, localStorageAgent);
                    return null;
                }
            } else {
                LOG.warn("Volume {} cannot be served by agent {}", storageVolume, localStorageAgent);
                return null;
            }
        }
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public List<JacsStorageVolume> findVolumes(StorageQuery storageQuery) {
        LOG.trace("Query managed volumes using {}", storageQuery);
        PageRequest pageRequest = new PageRequest();
        List<JacsStorageVolume> storageVolumes = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        Predicate<JacsStorageVolume> canAccessVolumePredicate = canAccessVolume();
        Predicate<JacsStorageVolume> canServeAndAccessVolumePredicate = canServeVolume(localStorageAgent).and(canAccessVolumePredicate.or(sv -> storageQuery.isIncludeInaccessibleVolumes()));
        return storageVolumes.stream()
                .filter(canServeAndAccessVolumePredicate)
                .peek(storageVolume -> {
                    if (canAccessVolumePredicate.test(storageVolume)) {
                        LOG.debug("Set access info for {} -> {}", storageVolume, localStorageAgent);
                        fillAccessInfo(storageVolume, localStorageAgent);
                    } else {
                        LOG.warn("Volume {} is not accessible on {}", storageVolume, localStorageAgent);
                        fillAccessInfo(storageVolume, NO_STORAGE_AGENT);
                    }
                })
                .collect(Collectors.toList());
    }

    @TimedMethod(
            logLevel = "trace",
            logResult = true
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        JacsStorageVolume updatedStorageVolume = super.updateVolumeInfo(volumeId, storageVolume);
        if (canServeVolume(localStorageAgent).and(canAccessVolume()).test(updatedStorageVolume)) {
            fillAccessInfo(updatedStorageVolume, localStorageAgent);
        } else {
            fillAccessInfo(updatedStorageVolume, NO_STORAGE_AGENT);
        }
        return updatedStorageVolume;
    }

    private Predicate<JacsStorageVolume> canServeVolume(JacsStorageAgent localStorageAgent) {
        return storageVolume -> {
            if (storageVolume.isShared()) {
                return JacsStorageVolume.OVERFLOW_VOLUME.equals(storageVolume.getName()) ||
                        localStorageAgent.canServe(storageVolume);
            } else {
                return localStorageAgent.getAgentHost().equals(storageVolume.getStorageAgentId()) &&
                        localStorageAgent.canServe(storageVolume);
            }
        };
    }

    private Predicate<JacsStorageVolume> canAccessVolume() {
        return storageVolume -> StringUtils.isNotBlank(storageVolume.getBaseStorageRootDir()) && Files.exists(Paths.get(storageVolume.getBaseStorageRootDir()));
    }

    private void fillAccessInfo(JacsStorageVolume storageVolume, JacsStorageAgent storageAgent) {
        storageVolume.setStorageServiceURL(storageAgent.getAgentAccessURL());
    }
}
