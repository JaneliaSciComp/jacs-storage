package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.utils.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public abstract class AbstractStorageAllocatorService implements StorageAllocatorService {

    protected final JacsStorageVolumeDao storageVolumeDao;
    protected final JacsBundleDao bundleDao;

    public AbstractStorageAllocatorService(JacsStorageVolumeDao storageVolumeDao, JacsBundleDao bundleDao) {
        this.storageVolumeDao = storageVolumeDao;
        this.bundleDao = bundleDao;
    }

    @Override
    public Optional<JacsBundle> allocateStorage(JacsBundle dataBundle) {
        return selectStorageVolume(dataBundle)
                .map((JacsStorageVolume storageVolume) -> {
                    dataBundle.setStorageVolumeId(storageVolume.getId());
                    dataBundle.setStorageVolume(storageVolume);
                    dataBundle.setConnectionInfo(storageVolume.getMountHostIP());
                    dataBundle.setConnectionURL(storageVolume.getMountHostURL());
                    bundleDao.save(dataBundle);
                    List<String> dataSubpath = PathUtils.getTreePathComponentsForId(dataBundle.getId());
                    Path dataPath = Paths.get(storageVolume.getMountPoint(), dataSubpath.toArray(new String[dataSubpath.size()]));
                    dataBundle.setPath(dataPath.toString());
                    bundleDao.update(dataBundle, ImmutableMap.of("path", new SetFieldValueHandler<>(dataBundle.getPath())));
                    return dataBundle;
                })
                ;
    }

    @Override
    public JacsBundle updateStorage(JacsBundle dataBundle) {
        JacsBundle existingBundle = retrieveExistingStorage(dataBundle);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBuilder = ImmutableMap.builder();
        if (dataBundle.hasUsedSpaceInKBSet()) {
            existingBundle.setUsedSpaceInKB(dataBundle.getUsedSpaceInKB());
            updatedFieldsBuilder.put("usedSpaceInKB", new SetFieldValueHandler<>(existingBundle.getUsedSpaceInKB()));
        }
        if (StringUtils.isNotBlank(dataBundle.getChecksum())) {
            existingBundle.setChecksum(dataBundle.getChecksum());
            updatedFieldsBuilder.put("checksum", new SetFieldValueHandler<>(existingBundle.getChecksum()));
        }
        bundleDao.update(existingBundle, updatedFieldsBuilder.build());
        return existingBundle;
    }

    protected JacsBundle retrieveExistingStorage(JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            throw new IllegalArgumentException("Invalid databundle id: " + dataBundle.getId());
        }
        return existingBundle;
    }

}
