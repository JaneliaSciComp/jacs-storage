package org.janelia.jacsstorage.service.impl.localservice;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import jakarta.annotation.Nullable;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@LocalInstance
@Dependent
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageVolumeManager.class);
    private static final JacsStorageAgent NO_STORAGE_AGENT = new JacsStorageAgent();
    private static final Integer FILL_UP_THRESHOLD = 85;

    private final AgentStatePersistence storageAgentPersistence;
    private final DataContentService dataContentService;
    private final NotificationService capacityNotifier;

    @Inject
    public LocalStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                     DataContentService dataContentService,
                                     AgentStatePersistence storageAgentPersistence,
                                     NotificationService capacityNotifier) {
        super(storageVolumeDao);
        this.dataContentService = dataContentService;
        this.storageAgentPersistence = storageAgentPersistence;
        this.capacityNotifier = capacityNotifier;
    }

    @TimedMethod
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

    @TimedMethod
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, JacsStorageType storageType, String storageAgentId) {
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        if (canServeVolume(localStorageAgent).test(new JacsStorageVolumeBuilder()
                .name(volumeName)
                .shared(StringUtils.isBlank(storageAgentId))
                .storageAgentId(storageAgentId)
                .build())) {
            // only create the volume if it can be served by this agent instance
            return super.createStorageVolumeIfNotFound(volumeName, storageType, storageAgentId);
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

    @TimedMethod
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
            logResult = true
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        JacsStorageVolume currentVolumeInfo = storageVolumeDao.findById(volumeId);
        Preconditions.checkArgument(currentVolumeInfo != null, "Invalid storage volume ID: " + volumeId);
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = super.updateVolumeFields(currentVolumeInfo, storageVolume);
        JacsStorageAgent localStorageAgent = storageAgentPersistence.getLocalStorageAgentInfo();
        if (canServeVolume(localStorageAgent).and(canAccessVolume()).test(currentVolumeInfo)) {
            fillAvailableSpace(currentVolumeInfo, updatedVolumeFields);
            fillAccessInfo(currentVolumeInfo, localStorageAgent);
        } else {
            fillAccessInfo(currentVolumeInfo, NO_STORAGE_AGENT);
        }
        if (updatedVolumeFields.isEmpty()) {
            return currentVolumeInfo;
        } else {
            return storageVolumeDao.update(currentVolumeInfo.getId(), updatedVolumeFields);
        }
    }

    private Predicate<JacsStorageVolume> canServeVolume(JacsStorageAgent localStorageAgent) {
        return storageVolume -> {
            if (storageVolume.isShared()) {
                return storageVolume.getStorageType() == JacsStorageType.S3 ||
                        localStorageAgent.canServe(storageVolume);
            } else {
                return localStorageAgent.getAgentHost().equals(storageVolume.getStorageAgentId()) &&
                        localStorageAgent.canServe(storageVolume);
            }
        };
    }

    private Predicate<JacsStorageVolume> canAccessVolume() {
        return storageVolume -> {
            if (!storageVolume.isActiveFlag()) {
                return false;
            }
            if (storageVolume.getStorageType() == JacsStorageType.S3) {
                return true;
            }
            return dataContentService.exists(storageVolume.getVolumeStorageRootURI());
        };
    }

    private void fillAccessInfo(JacsStorageVolume storageVolume, JacsStorageAgent storageAgent) {
        storageVolume.setStorageServiceURL(storageAgent.getAgentAccessURL());
    }

    private void fillAvailableSpace(JacsStorageVolume storageVolume, Map<String, EntityFieldValueHandler<?>> updatedVolumeFields) {
        StorageCapacity storageCapacity = getStorageCapacity(storageVolume.getVolumeStorageRootURI());
        if (storageCapacity.getUsableSpace() != -1 && !Long.valueOf(storageCapacity.getUsableSpace()).equals(storageVolume.getAvailableSpaceInBytes())) {
            LOG.trace("Update availableSpace for volume {}:{} to {} bytes", storageVolume.getId(), storageVolume.getName(), storageCapacity.getUsableSpace());
            storageVolume.setAvailableSpaceInBytes(storageCapacity.getUsableSpace());
            updatedVolumeFields.put("availableSpaceInBytes", new SetFieldValueHandler<>(storageCapacity.getUsableSpace()));
        }
        if (storageCapacity.getTotalSpace() > 0 && storageCapacity.getUsableSpace() != -1) {
            Integer currentPercentage = storageVolume.getPercentageFull();
            int newUsagePercentage = (int) ((storageCapacity.getTotalSpace() - storageCapacity.getUsableSpace()) * 100. / storageCapacity.getTotalSpace());
            if (!Integer.valueOf(newUsagePercentage).equals(currentPercentage)) {
                LOG.trace("Update percentageFull for volume {}:{} to {}%", storageVolume.getId(), storageVolume.getName(), newUsagePercentage);
                storageVolume.setPercentageFull(newUsagePercentage);
                updatedVolumeFields.put("percentageFull", new SetFieldValueHandler<>(newUsagePercentage));
                notifyCapacityChange(
                        currentPercentage, newUsagePercentage,
                        storageVolume.isShared()
                                ? storageVolume.getName()
                                : storageVolume.getName() + " on " + storageVolume.getStorageAgentId(),
                        storageVolume.getId());
            }
        }
    }

    private StorageCapacity getStorageCapacity(@Nullable JADEStorageURI storageURI) {
        if (storageURI == null) {
            return new StorageCapacity(-1, -1);
        } else {
            return dataContentService.storageCapacity(storageURI);
        }
    }

    private void notifyCapacityChange(Integer previousUsagePercentage, Integer newUsagePercentage, String volumeLocation, Number volumeId) {
        // check if it just crossed the threshold up or down
        if (newUsagePercentage > FILL_UP_THRESHOLD && previousUsagePercentage != null && previousUsagePercentage <= FILL_UP_THRESHOLD) {
            // it just crossed the threshold up
            LOG.warn("Volume {}({}) is {}% full and it just passed the threshold of {}%", volumeLocation, volumeId, newUsagePercentage, FILL_UP_THRESHOLD);
            String capacityNotification = "Volume " + volumeLocation +
                    " just passed the fillup percentage threshold of " + FILL_UP_THRESHOLD + " and it currently is at " + newUsagePercentage + "%";
            capacityNotifier.sendNotification(
                    "Volume " + volumeLocation + " is above fill up threshold",
                    capacityNotification);
        } else if (newUsagePercentage <= FILL_UP_THRESHOLD && previousUsagePercentage != null && previousUsagePercentage > FILL_UP_THRESHOLD) {
            // it just crossed the threshold down
            LOG.info("Volume {}({}) is {}% full and it just dropped below the threshold of {}%", volumeLocation, volumeId, newUsagePercentage, FILL_UP_THRESHOLD);
            String capacityNotification = "Volume " + volumeLocation +
                    " just dropped below percentage threshold of " + FILL_UP_THRESHOLD + " and it currently is at " + newUsagePercentage + "%";
            capacityNotifier.sendNotification(
                    "Volume " + volumeLocation + " is below the threshold now",
                    capacityNotification);
        }
    }
}
