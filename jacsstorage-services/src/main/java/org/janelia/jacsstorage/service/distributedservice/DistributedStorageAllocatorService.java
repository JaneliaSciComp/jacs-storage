package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.AbstractStorageAllocatorService;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

@RemoteInstance
public class DistributedStorageAllocatorService extends AbstractStorageAllocatorService {
    private static final Logger LOG = LoggerFactory.getLogger(DistributedStorageAllocatorService.class);

    private final StorageAgentManager agentManager;

    @Inject
    public DistributedStorageAllocatorService(JacsStorageVolumeDao storageVolumeDao,
                                              JacsBundleDao bundleDao,
                                              StorageAgentManager agentManager) {
        super(storageVolumeDao, bundleDao);
        this.agentManager = agentManager;
    }

    @Override
    public boolean deleteStorage(JacsCredentials credentials, JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            return false;
        }
        checkStorageAccess(credentials, existingBundle);
        return existingBundle.setStorageVolume(storageVolumeDao.findById(existingBundle.getStorageVolumeId()))
                .flatMap(sv -> agentManager.findRegisteredAgentByLocationOrConnectionInfo(sv.getLocation()))
                .map(storageAgentInfo -> {
                    if (AgentConnectionHelper.deleteStorage(storageAgentInfo.getAgentURL(), existingBundle.getId(), credentials.getAuthToken())) {
                        LOG.info("Delete {}", existingBundle);
                        bundleDao.delete(existingBundle);
                        return true;
                    } else {
                        return false;
                    }
                })
                .orElse(false);
    }

    @Override
    public Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle) {
        Predicate<StorageAgentInfo> spaceAvailableCondition = (StorageAgentInfo sai) -> dataBundle.getUsedSpaceInBytes() == null || sai.getStorageSpaceAvailableInBytes() > dataBundle.getUsedSpaceInBytes();
        return agentManager.findRandomRegisteredAgent(spaceAvailableCondition)
                .map((StorageAgentInfo storageAgentInfo) -> {
                    JacsStorageVolume storageVolume = storageVolumeDao.getStorageByLocationAndCreateIfNotFound(storageAgentInfo.getLocation());
                    ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
                    if (StringUtils.isBlank(storageVolume.getMountPoint())) {
                        storageVolume.setMountPoint(storageAgentInfo.getStoragePath());
                        updatedVolumeFieldsBuilder.put("mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()));
                    }
                    if (!StorageAgentInfo.OVERFLOW_AGENT.equals(storageVolume.getLocation())) {
                        if (StringUtils.isBlank(storageVolume.getMountHostIP())) {
                            storageVolume.setMountHostIP(storageAgentInfo.getConnectionInfo());
                            updatedVolumeFieldsBuilder.put("mountHostIP", new SetFieldValueHandler<>(storageVolume.getMountHostIP()));
                        }
                        if (StringUtils.isBlank(storageVolume.getMountHostURL())) {
                            storageVolume.setMountHostURL(storageAgentInfo.getAgentURL());
                            updatedVolumeFieldsBuilder.put("mountHostURL", new SetFieldValueHandler<>(storageVolume.getMountHostURL()));
                        }
                    } else {
                        // if the location is the overflow location - set the host connection info but do not update the persisted entity
                        storageVolume.setMountHostIP(storageAgentInfo.getConnectionInfo());
                        storageVolume.setMountHostURL(storageAgentInfo.getAgentURL());
                    }
                    storageVolumeDao.update(storageVolume, updatedVolumeFieldsBuilder.build());
                    return storageVolume;
                })
                ;
    }
}
