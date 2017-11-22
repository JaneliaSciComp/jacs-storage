package org.janelia.jacsstorage.service.localservice;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.AbstractStorageAllocatorService;
import org.janelia.jacsstorage.service.OverflowStorageVolumeSelector;
import org.janelia.jacsstorage.service.StorageVolumeSelector;
import org.janelia.jacsstorage.utils.NetUtils;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
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
                        Path parentPath = Paths.get(storageVolume.getStorageRootDir(), dataSubpath.get(0));
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
        StorageVolumeSelector[] volumeSelectors = new StorageVolumeSelector[] {
                new LocalStorageVolumeSelector(storageVolumeDao, getStorageHost()),
                new OverflowStorageVolumeSelector(storageVolumeDao)
        };
        JacsStorageVolume storageVolume = null;
        for (StorageVolumeSelector volumeSelector : volumeSelectors) {
            storageVolume = volumeSelector.selectStorageVolume(dataBundle);
            if (storageVolume != null) {
                break;
            }
        }
        if (storageVolume == null) {
            LOG.warn("No storage volume selected for {}", dataBundle);
            return Optional.empty();
        } else {
            return Optional.of(storageVolume);
        }
    }

    private String getStorageHost() {
        return StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

}
