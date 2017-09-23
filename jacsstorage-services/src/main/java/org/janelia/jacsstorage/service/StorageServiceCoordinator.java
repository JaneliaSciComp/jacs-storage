package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.utils.PathUtils;

import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class StorageServiceCoordinator implements StorageService {

    private final StorageAgentManager agentManager;
    private final JacsStorageVolumeDao storageVolumeDao;
    private final JacsBundleDao bundleDao;

    @Inject
    public StorageServiceCoordinator(StorageAgentManager agentManager, JacsStorageVolumeDao storageVolumeDao, JacsBundleDao bundleDao) {
        this.agentManager = agentManager;
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
    }

    @Override
    public Optional<JacsBundle> allocateStorage(JacsBundle dataBundle) {
        return agentManager.findRandomRegisteredAgent((StorageAgentInfo sai) -> dataBundle.getUsedSpaceInKB() == null || sai.getStorageSpaceAvailableInMB() * 1000 > dataBundle.getUsedSpaceInKB())
                .map((StorageAgentInfo  storageAgentInfo) -> {
                    JacsStorageVolume storageVolume = storageVolumeDao.getStorageByLocation(storageAgentInfo.getLocation());
                    if (StringUtils.isBlank(storageVolume.getMountPoint())) {
                        storageVolume.setMountHostIP(storageAgentInfo.getConnectionInfo());
                        storageVolume.setMountPoint(storageAgentInfo.getStoragePath());
                        storageVolumeDao.update(storageVolume, ImmutableMap.of(
                                        "mountHostIP", new SetFieldValueHandler<>(storageVolume.getMountHostIP()),
                                        "mountPoint", new SetFieldValueHandler<>(storageVolume.getMountPoint()))
                        );
                    }
                    dataBundle.setStorageVolumeId(storageVolume.getId());
                    dataBundle.setStorageVolume(storageVolume);
                    dataBundle.setConnectionInfo(storageAgentInfo.getConnectionInfo());
                    bundleDao.save(dataBundle);
                    List<String> dataSubpath = PathUtils.getTreePathComponentsForId(dataBundle.getId());
                    Path dataPath = Paths.get(storageVolume.getMountPoint(), dataSubpath.toArray(new String[dataSubpath.size()]));
                    dataBundle.setPath(dataPath.toString());
                    bundleDao.update(dataBundle, ImmutableMap.of("path", new SetFieldValueHandler<>(dataBundle.getPath())));
                    return dataBundle;
                });
    }

    @Override
    public JacsBundle getDataBundleById(Number id) {
        JacsBundle bundle = bundleDao.findById(id);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    @Override
    public JacsBundle findDataBundleByOwnerAndName(String owner, String name) {
        JacsBundle bundle = bundleDao.findByOwnerAndName(owner, name);
        if (bundle != null) {
            updateStorageVolume(bundle);
        }
        return bundle;
    }

    private void updateStorageVolume(JacsBundle bundle) {
        bundle.setStorageVolume(storageVolumeDao.findById(bundle.getStorageVolumeId()))
                .flatMap(storageVolume -> agentManager.findRegisteredAgentByLocationOrConnectionInfo(storageVolume.getLocation())) // find a registered agent that serves the given location
                .map(storageAgent -> {
                    bundle.setConnectionInfo(storageAgent.getConnectionInfo());
                    return bundle;
                });
    }

    @Override
    public JacsBundle updateDataBundle(JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            return null;
        }
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBuilder = ImmutableMap.<String, EntityFieldValueHandler<?>>builder();
        if (dataBundle.hasUsedSpaceInKBSet()) {
            if (existingBundle.hasUsedSpaceInKBSet()) {
                existingBundle.setUsedSpaceInKB(existingBundle.getUsedSpaceInKB() + dataBundle.getUsedSpaceInKB());
            } else {
                existingBundle.setUsedSpaceInKB(dataBundle.getUsedSpaceInKB());
            }
            updatedFieldsBuilder.put("usedSpaceInKB", new SetFieldValueHandler<>(existingBundle.getUsedSpaceInKB()));
        }
        bundleDao.update(dataBundle, updatedFieldsBuilder.build());
        return existingBundle;
    }
}
