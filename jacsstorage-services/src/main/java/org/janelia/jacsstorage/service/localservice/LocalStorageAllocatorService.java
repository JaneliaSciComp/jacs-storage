package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.DataInterval;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.AbstractStorageAllocatorService;
import org.janelia.jacsstorage.utils.NetUtils;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

@LocalInstance
public class LocalStorageAllocatorService extends AbstractStorageAllocatorService {
    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageAllocatorService.class);

    private final JacsStorageVolumeDao storageVolumeDao;
    private final String storageHost;

    @Inject
    public LocalStorageAllocatorService(JacsStorageVolumeDao storageVolumeDao, JacsBundleDao bundleDao,
                                        @PropertyValue(name = "StorageAgent.StorageHost") String storageHost) {
        super(bundleDao);
        this.storageVolumeDao = storageVolumeDao;
        this.storageHost = storageHost;
    }

    @Override
    public boolean deleteStorage(JacsCredentials credentials, JacsBundle dataBundle) {
        JacsBundle existingBundle = retrieveExistingStorage(dataBundle);
        checkStorageAccess(credentials, dataBundle);
        LOG.info("Delete {}", existingBundle);
        return existingBundle.setStorageVolume(storageVolumeDao.findById(existingBundle.getStorageVolumeId()))
                .map(storageVolume -> {
                    Path dataPath = Paths.get(existingBundle.getPath());
                    try {
                        PathUtils.deletePath(dataPath);
                    } catch (IOException e) {
                        LOG.error("Error deleting {}", dataPath, e);
                        return false;
                    }
                    List<String> dataSubpath = PathUtils.getTreePathComponentsForId(existingBundle.getId());
                    if (CollectionUtils.isNotEmpty(dataSubpath)) {
                        Path parentPath = Paths.get(storageVolume.getVolumePath(), dataSubpath.get(0));
                        if (dataPath.startsWith(parentPath)) {
                            try {
                                PathUtils.deletePathIfEmpty(parentPath);
                            } catch (IOException e) {
                                LOG.error("Error cleaning {}", parentPath, e);
                                return false;
                            }
                        }
                    }
                    return true;
                })
                .orElse(false);
    }

    @Override
    public Optional<JacsStorageVolume> selectStorageVolume(JacsBundle dataBundle) {
        JacsStorageVolume volumePattern = new JacsStorageVolume();
        volumePattern.setStorageHost(getStorageHost());
        volumePattern.setStorageTags(dataBundle.getStorageTags());
        dataBundle.getStorageVolume()
                .ifPresent(sv -> {
                    volumePattern.setId(sv.getId());
                    volumePattern.setName(sv.getName());
                });
        if (dataBundle.hasUsedSpaceSet()) {
            volumePattern.setAvailableSpaceInBytes(dataBundle.getUsedSpaceInBytes());
        }
        PageResult<JacsStorageVolume> storageVolumeResults = storageVolumeDao.findMatchingVolumes(volumePattern, new PageRequest());
        if (storageVolumeResults.getResultList().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(storageVolumeResults.getResultList().get(0));
        }
    }

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostIP());
    }

}
