package org.janelia.jacsstorage.service.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.IncFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.service.StorageAllocatorService;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public abstract class AbstractStorageAllocatorService implements StorageAllocatorService {

    protected final JacsBundleDao bundleDao;

    public AbstractStorageAllocatorService(JacsBundleDao bundleDao) {
        this.bundleDao = bundleDao;
    }

    @TimedMethod
    @Override
    public Optional<JacsBundle> allocateStorage(String dataBundlePathPrefix, JacsBundle dataBundle, JacsCredentials credentials) {
        return selectStorageVolume(dataBundle)
                .map((JacsStorageVolume storageVolume) -> createStorage(credentials, storageVolume, dataBundlePathPrefix, dataBundle))
                ;
    }

    private JacsBundle createStorage(JacsCredentials credentials, JacsStorageVolume storageVolume, String dataBundlePathPrefix, JacsBundle dataBundle) {
        try {
            dataBundle.setOwnerKey(credentials.getSubjectKey());
            dataBundle.setStorageVolumeId(storageVolume.getId());
            dataBundle.setStorageVolume(storageVolume);
            dataBundle.setCreatedBy(credentials.getAuthKey());
            bundleDao.save(dataBundle);
            List<String> dataSubpath = PathUtils.getTreePathComponentsForId(dataBundle.getId());
            Path dataPath = Paths.get(StringUtils.defaultIfBlank(dataBundlePathPrefix, ""), dataSubpath.toArray(new String[dataSubpath.size()]));
            dataBundle.setPath(dataPath.toString());
            JacsBundle updatedBundle = bundleDao.update(dataBundle.getId(), ImmutableMap.of("path", new SetFieldValueHandler<>(dataBundle.getPath())));
            updatedBundle.setStorageVolume(storageVolume);
            return updatedBundle;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public JacsBundle updateStorage(JacsBundle dataBundle, JacsCredentials credentials) {
        JacsBundle existingBundle = retrieveExistingStorage(dataBundle);
        checkStorageWriteAccess(existingBundle, credentials);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBuilder = ImmutableMap.builder();
        if (dataBundle.hasUsedSpaceSet()) {
            updatedFieldsBuilder.put("usedSpaceInBytes", new IncFieldValueHandler<>(dataBundle.getUsedSpaceInBytes()));
        }
        if (StringUtils.isNotBlank(dataBundle.getChecksum())) {
            updatedFieldsBuilder.put("checksum", new SetFieldValueHandler<>(dataBundle.getChecksum()));
        }
        return bundleDao.update(existingBundle.getId(), updatedFieldsBuilder.build());
    }

    @TimedMethod
    protected JacsBundle retrieveExistingStorage(JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            throw new IllegalArgumentException("Invalid databundle id: " + dataBundle.getId());
        }
        return existingBundle;
    }

    private void checkStorageWriteAccess(JacsBundle dataBundle, JacsCredentials credentials) {
        if (!dataBundle.hasWritePermissions(credentials.getSubjectKey())) {
            throw new SecurityException("Access not allowed to " + dataBundle.getName() + " for " + credentials.getAuthKey() + " as " + credentials.getName());
        }
    }

    protected void checkStorageDeletePermission(JacsBundle dataBundle, JacsCredentials credentials) {
        if (!credentials.getSubjectKey().equals(dataBundle.getOwnerKey())) {
            throw new SecurityException("Access not allowed to " + dataBundle.getName() + " for " + credentials.getAuthKey() + " as " + credentials.getName());
        }
    }
}
