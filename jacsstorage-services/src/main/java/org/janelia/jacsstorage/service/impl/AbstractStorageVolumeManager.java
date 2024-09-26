package org.janelia.jacsstorage.service.impl;

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.StorageVolumeManager;

public abstract class AbstractStorageVolumeManager implements StorageVolumeManager {


    protected final JacsStorageVolumeDao storageVolumeDao;

    protected AbstractStorageVolumeManager(JacsStorageVolumeDao storageVolumeDao) {
        this.storageVolumeDao = storageVolumeDao;
    }

    @TimedMethod
    @Override
    public JacsStorageVolume createNewStorageVolume(JacsStorageVolume storageVolume) {
        storageVolumeDao.save(storageVolume);
        return storageVolume;
    }

    @TimedMethod
    @Override
    public JacsStorageVolume createStorageVolumeIfNotFound(String volumeName, JacsStorageType storageType, String storageAgentId) {
        return storageVolumeDao.createStorageVolumeIfNotFound(volumeName, storageType, storageAgentId);
    }

    protected Map<String, EntityFieldValueHandler<?>> updateVolumeFields(JacsStorageVolume prevVolumeInfo, JacsStorageVolume newStorageVolumeInfo) {
        Map<String, EntityFieldValueHandler<?>> updatedVolumeFields = new HashMap<>();
        if (prevVolumeInfo.getStorageRootTemplate() == null ||
                (StringUtils.isNotBlank(newStorageVolumeInfo.getStorageRootTemplate()) &&
                        !newStorageVolumeInfo.getStorageRootTemplate().equals(prevVolumeInfo.getStorageRootTemplate()))) {
            prevVolumeInfo.setStorageRootTemplate(newStorageVolumeInfo.getStorageRootTemplate());
            updatedVolumeFields.put("storageRootTemplate", new SetFieldValueHandler<>(newStorageVolumeInfo.getStorageRootTemplate()));
        }
        if (prevVolumeInfo.getStorageVirtualPath() == null ||
                (StringUtils.isNotBlank(newStorageVolumeInfo.getStorageVirtualPath()) &&
                        !newStorageVolumeInfo.getStorageVirtualPath().equals(prevVolumeInfo.getStorageVirtualPath()))) {
            prevVolumeInfo.setStorageVirtualPath(newStorageVolumeInfo.getStorageVirtualPath());
            updatedVolumeFields.put("storageVirtualPath", new SetFieldValueHandler<>(newStorageVolumeInfo.getStorageVirtualPath()));
        }
        if (newStorageVolumeInfo.isNotShared()) {
            if (StringUtils.isNotBlank(newStorageVolumeInfo.getStorageServiceURL()) &&
                    (StringUtils.isBlank(prevVolumeInfo.getStorageServiceURL()) ||
                            !newStorageVolumeInfo.getStorageServiceURL().equals(prevVolumeInfo.getStorageServiceURL()))) {
                // if the storage access URLs differ
                prevVolumeInfo.setStorageServiceURL(newStorageVolumeInfo.getStorageServiceURL());
                updatedVolumeFields.put("storageServiceURL", new SetFieldValueHandler<>(newStorageVolumeInfo.getStorageServiceURL()));
            }
        } else if (prevVolumeInfo.isNotShared()) {
            // if somehow the current volume is not shared make it shared
            prevVolumeInfo.setShared(true);
            prevVolumeInfo.setStorageServiceURL(null);
            prevVolumeInfo.setStorageAgentId(null);
            updatedVolumeFields.put("shared", new SetFieldValueHandler<>(true));
            updatedVolumeFields.put("storageServiceURL", new SetFieldValueHandler<>(null));
            updatedVolumeFields.put("storageHost", new SetFieldValueHandler<>(null));
        }

        if (!prevVolumeInfo.hasTags() && newStorageVolumeInfo.hasTags() ||
                prevVolumeInfo.hasTags() && !newStorageVolumeInfo.hasTags() ||
                prevVolumeInfo.hasTags() && newStorageVolumeInfo.hasTags() && !ImmutableSet.copyOf(prevVolumeInfo.getStorageTags()).equals(ImmutableSet.copyOf(newStorageVolumeInfo.getStorageTags()))) {
            // if the tags differ
            prevVolumeInfo.setStorageTags(newStorageVolumeInfo.getStorageTags());
            updatedVolumeFields.put("storageTags", new SetFieldValueHandler<>(newStorageVolumeInfo.getStorageTags()));
        }
        if (!prevVolumeInfo.hasPermissions() && newStorageVolumeInfo.hasPermissions() ||
                prevVolumeInfo.hasPermissions() && !newStorageVolumeInfo.hasPermissions() ||
                prevVolumeInfo.hasPermissions() && newStorageVolumeInfo.hasPermissions() && !prevVolumeInfo.getVolumePermissions().equals(newStorageVolumeInfo.getVolumePermissions())) {
            // if the permissions differ
            prevVolumeInfo.setVolumePermissions(newStorageVolumeInfo.getVolumePermissions());
            updatedVolumeFields.put("volumePermissions", new SetFieldValueHandler<>(newStorageVolumeInfo.getVolumePermissions()));
        }
        if (newStorageVolumeInfo.getQuotaFailPercent() != null && !newStorageVolumeInfo.getQuotaFailPercent().equals(prevVolumeInfo.getQuotaFailPercent())) {
            prevVolumeInfo.setQuotaFailPercent(newStorageVolumeInfo.getQuotaFailPercent());
            updatedVolumeFields.put("quotaFailPercent", new SetFieldValueHandler<>(newStorageVolumeInfo.getQuotaFailPercent()));
        }
        if (newStorageVolumeInfo.getQuotaWarnPercent() != null && !newStorageVolumeInfo.getQuotaWarnPercent().equals(prevVolumeInfo.getQuotaWarnPercent())) {
            prevVolumeInfo.setQuotaWarnPercent(newStorageVolumeInfo.getQuotaWarnPercent());
            updatedVolumeFields.put("quotaWarnPercent", new SetFieldValueHandler<>(newStorageVolumeInfo.getQuotaWarnPercent()));
        }
        if (StringUtils.isNotBlank(newStorageVolumeInfo.getSystemUsageFile()) && !newStorageVolumeInfo.getSystemUsageFile().equals(prevVolumeInfo.getSystemUsageFile())) {
            prevVolumeInfo.setSystemUsageFile(newStorageVolumeInfo.getSystemUsageFile());
            updatedVolumeFields.put("systemUsageFile", new SetFieldValueHandler<>(newStorageVolumeInfo.getSystemUsageFile()));
        }
        if (prevVolumeInfo.isActiveFlag() != newStorageVolumeInfo.isActiveFlag()) {
            prevVolumeInfo.setActiveFlag(newStorageVolumeInfo.isActiveFlag());
            updatedVolumeFields.put("activeFlag", new SetFieldValueHandler<>(newStorageVolumeInfo.isActiveFlag()));
        }
        return updatedVolumeFields;
    }

}
