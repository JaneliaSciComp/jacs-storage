package org.janelia.jacsstorage.service.localservice;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.impl.AbstractStorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

@LocalInstance
public class LocalStorageVolumeManager extends AbstractStorageVolumeManager {

    private static final Logger LOG = LoggerFactory.getLogger(LocalStorageVolumeManager.class);

    private final String storageHost;

    @Inject
    public LocalStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao,
                                     NotificationService capacityNotifier,
                                     @PropertyValue(name = "StorageAgent.StorageHost") String storageHost) {
        super(storageVolumeDao, capacityNotifier);
        this.storageHost = StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

    @TimedMethod
    @Override
    public Optional<JacsStorageVolume> getFullVolumeInfo(String volumeName) {
        return getManagedVolumes(new StorageQuery().setStorageName(volumeName).setAccessibleOnHost(storageHost)).stream()
                .findFirst()
                .map(sv -> {
                    fillVolumeSpaceInfo(sv);
                    return sv;
                });
    }

    private void fillVolumeSpaceInfo(JacsStorageVolume storageVolume) {
        String storageRootDir = ExprHelper.getConstPrefix(storageVolume.getStorageRootTemplate());
        if (StringUtils.isBlank(storageRootDir)) {
            LOG.warn("Don't know how to get the total available space for {}", storageVolume);
            return;
        }
        storageVolume.setAvailableSpaceInBytes(getAvailableStorageSpaceInBytes(storageRootDir));
        long totalSpace = getTotalStorageSpaceInBytes(storageRootDir);
        if (totalSpace != 0) {
            storageVolume.setPercentageFull((int) ((totalSpace - storageVolume.getAvailableSpaceInBytes()) * 100 / totalSpace));
        } else {
            LOG.warn("Total space calculated is 0");
        }
    }

    @TimedMethod
    @Override
    public List<JacsStorageVolume> getManagedVolumes(StorageQuery storageQuery) {
        storageQuery.setAccessibleOnHost(storageHost);
        LOG.trace("Query managed volumes using {}", storageQuery);
        PageRequest pageRequest = new PageRequest();
        return storageVolumeDao.findMatchingVolumes(storageQuery, pageRequest).getResultList();
    }

    private long getTotalStorageSpaceInBytes(String storageDirName) {
        return getFileStore(storageDirName)
                .map(fs -> {
                    try {
                        return fs.getTotalSpace();
                    } catch (IOException e) {
                        LOG.error("Error trying to get total storage space {}", storageDirName, e);
                        return 0L;
                    }
                })
                .orElse(0L)
                ;
    }

}
