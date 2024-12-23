package org.janelia.jacsstorage.service.impl.distributedservice;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Inject;

import com.google.common.base.Preconditions;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RemoteInstance
@Dependent
public class DistributedStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(DistributedStorageVolumeManager.class);

    private final DistributedStorageHelper storageHelper;

    @Inject
    public DistributedStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                           StorageAgentManager agentManager) {
        super(storageVolumeDao);
        this.storageHelper = new DistributedStorageHelper(agentManager);
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume) {
        JacsStorageVolume newStorageVolume = super.createNewStorageVolume(storageVolume);
        storageHelper.fillStorageAccessInfo(newStorageVolume);
        return newStorageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, JacsStorageType jacsStorageType, String storageAgentId) {
        JacsStorageVolume newStorageVolume = super.createStorageVolumeIfNotFound(volumeName, jacsStorageType, storageAgentId);
        storageHelper.fillStorageAccessInfo(newStorageVolume);
        return newStorageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public JacsStorageVolume getVolumeById(Number volumeId) {
        JacsStorageVolume storageVolume = storageVolumeDao.findById(volumeId);
        storageHelper.fillStorageAccessInfo(storageVolume);
        return storageVolume;
    }

    @TimedMethod(
            logLevel = "info"
    )
    @Override
    public List<JacsStorageVolume> findVolumes(StorageQuery storageQuery) {
        LOG.info("Lookup volumes using: {}", storageQuery);
        PageRequest pageRequest = new PageRequest();
        List<JacsStorageVolume> managedVolumes = storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
        Predicate<JacsStorageVolume> filteringPredicate;
        if (storageQuery.isIncludeInaccessibleVolumes()) {
            filteringPredicate = sv -> true;
        } else {
            filteringPredicate = sv -> storageHelper.isAccessible(sv);
        }
        return managedVolumes.stream()
                .filter(filteringPredicate)
                .peek(storageHelper::fillStorageAccessInfo)
                .collect(Collectors.toList());
    }

    @TimedMethod(
            logLevel = "trace"
    )
    @Override
    public JacsStorageVolume updateVolumeInfo(Number volumeId, JacsStorageVolume storageVolume) {
        JacsStorageVolume currentVolumeInfo = storageVolumeDao.findById(volumeId);
        Preconditions.checkArgument(currentVolumeInfo != null, "Invalid storage volume ID: " + volumeId);
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = super.updateVolumeFields(currentVolumeInfo, storageVolume);
        storageHelper.fillStorageAccessInfo(currentVolumeInfo);
        if (updatedVolumeFields.isEmpty()) {
            return currentVolumeInfo;
        } else {
            return storageVolumeDao.update(currentVolumeInfo.getId(), updatedVolumeFields);
        }
    }
}
