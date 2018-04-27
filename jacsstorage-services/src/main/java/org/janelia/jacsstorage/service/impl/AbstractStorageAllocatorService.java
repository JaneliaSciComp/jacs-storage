package org.janelia.jacsstorage.service.impl;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.IncFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.coreutils.PathUtils;
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
            dataBundle.setCreatedBy(credentials.getAuthSubject());
            bundleDao.save(dataBundle);
            List<String> dataSubpath = PathUtils.getTreePathComponentsForId(dataBundle.getId());
            Path dataPath = Paths.get(StringUtils.defaultIfBlank(dataBundlePathPrefix, ""), dataSubpath.toArray(new String[dataSubpath.size()]));
            dataBundle.setPath(dataPath.toString());
            bundleDao.update(dataBundle, ImmutableMap.of("path", new SetFieldValueHandler<>(dataBundle.getPath())));
            return dataBundle;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public JacsBundle updateStorage(JacsBundle dataBundle, JacsCredentials credentials) {
        JacsBundle existingBundle = retrieveExistingStorage(dataBundle);
        checkStorageWriteAccess(credentials, existingBundle);
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBuilder = ImmutableMap.builder();
        if (dataBundle.hasUsedSpaceSet()) {
            existingBundle.setUsedSpaceInBytes(dataBundle.getUsedSpaceInBytes());
            updatedFieldsBuilder.put("usedSpaceInBytes", new IncFieldValueHandler<>(existingBundle.getUsedSpaceInBytes()));
        }
        if (StringUtils.isNotBlank(dataBundle.getChecksum())) {
            existingBundle.setChecksum(dataBundle.getChecksum());
            updatedFieldsBuilder.put("checksum", new SetFieldValueHandler<>(existingBundle.getChecksum()));
        }
        if (dataBundle.hasMetadata()) {
            existingBundle.addMetadataFields(dataBundle.getMetadata());
            updatedFieldsBuilder.put("metadata", new SetFieldValueHandler<>(existingBundle.getMetadata()));
        }
        bundleDao.update(existingBundle, updatedFieldsBuilder.build());
        return existingBundle;
    }

    @TimedMethod
    protected JacsBundle retrieveExistingStorage(JacsBundle dataBundle) {
        JacsBundle existingBundle = bundleDao.findById(dataBundle.getId());
        if (existingBundle == null) {
            throw new IllegalArgumentException("Invalid databundle id: " + dataBundle.getId());
        }
        return existingBundle;
    }

    private void checkStorageWriteAccess(JacsCredentials credentials, JacsBundle dataBundle) {
        if (!dataBundle.hasWritePermissions(credentials.getSubjectKey())) {
            throw new SecurityException("Access not allowed to " + dataBundle.getName() + " for " + credentials.getAuthSubject() + " as " + credentials.getName());
        }
    }

    protected void checkStorageDeletePermission(JacsCredentials credentials, JacsBundle dataBundle) {
        if (!credentials.getSubjectKey().equals(dataBundle.getOwnerKey())) {
            throw new SecurityException("Access not allowed to " + dataBundle.getName() + " for " + credentials.getAuthSubject() + " as " + credentials.getName());
        }
    }
}
