package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
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

    private final String storageLocation;
    private final String storageIPAddress;
    private final String storageRootDir;
    private final String overflowRootDir;


    @Inject
    public LocalStorageAllocatorService(JacsStorageVolumeDao storageVolumeDao, JacsBundleDao bundleDao,
                                        @PropertyValue(name = "StorageAgent.agentLocation") String storageLocation,
                                        @PropertyValue(name = "StorageAgent.IPAddress") String storageIPAddress,
                                        @PropertyValue(name = "StorageAgent.storageRootDir") String storageRootDir,
                                        @PropertyValue(name = "Storage.Overflow.RootDir") String overflowRootDir) {
        super(storageVolumeDao, bundleDao);
        this.storageLocation = storageLocation;
        this.storageIPAddress = storageIPAddress;
        this.storageRootDir = storageRootDir;
        this.overflowRootDir = overflowRootDir;
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
                        Path parentPath = Paths.get(storageVolume.getMountPoint(), dataSubpath.get(0));
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
        long availableSpace = getAvailableStorageSpace();
        String selectedStorageLocation;
        String rootDir;
        if (availableSpace > 0 &&
                (dataBundle.getUsedSpaceInBytes() == null || availableSpace > dataBundle.getUsedSpaceInBytes())) {
            selectedStorageLocation = getStorageLocation();
            rootDir = storageRootDir;
        } else {
            selectedStorageLocation = StorageAgentInfo.OVERFLOW_AGENT;
            rootDir = overflowRootDir;
        }
        JacsStorageVolume storageVolume = storageVolumeDao.getStorageByLocationAndCreateIfNotFound(selectedStorageLocation);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedVolumeFieldsBuilder = ImmutableMap.builder();
        if (StringUtils.isBlank(storageVolume.getMountPoint())) {
            storageVolume.setMountPoint(rootDir);
            updatedVolumeFieldsBuilder.put("mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()));
        }
        storageVolumeDao.update(storageVolume, updatedVolumeFieldsBuilder.build());
        return Optional.of(storageVolume);
    }

    private String getStorageLocation() {
        return StringUtils.isBlank(storageLocation) ? getStorageIPAddress() + "/" + storageRootDir : storageLocation;
    }

    private String getStorageIPAddress() {
        return StringUtils.isBlank(storageIPAddress) ? getCurrentHostIP() : storageIPAddress;
    }

    private String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long getAvailableStorageSpace() {
        try {
            java.nio.file.Path storageRootPath = Paths.get(storageRootDir);
            FileStore storageRootStore = Files.getFileStore(storageRootPath);
            long usableBytes = storageRootStore.getUsableSpace();
            return usableBytes;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
