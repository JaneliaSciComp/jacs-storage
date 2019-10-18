package org.janelia.jacsstorage.service;

import java.util.Set;

import javax.inject.Inject;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.dao.JacsStorageAgentDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

public class AgentStatePersistence {

    private final JacsStorageAgentDao jacsStorageAgentDao;
    private final String storageAgentId;

    @Inject
    public AgentStatePersistence(JacsStorageAgentDao jacsStorageAgentDao,
                                 @PropertyValue(name = "StorageAgent.StorageHost") String storageHost,
                                 @PropertyValue(name = "StorageAgent.StoragePortNumber") String storagePort) {
        this.jacsStorageAgentDao = jacsStorageAgentDao;
        this.storageAgentId = NetUtils.createStorageHostId(
                StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName()),
                storagePort
        );
    }

    public JacsStorageAgent createAgentStorage(String storageAgentId, String agentAccessURL, String status) {
        return jacsStorageAgentDao.createStorageAgentIfNotFound(storageAgentId, agentAccessURL, status, ImmutableSet.of("*"));
    }

    public void updateAgentServedVolumes(JacsStorageAgent jacsStorageAgent) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBulder = ImmutableMap.builder();
        updatedFieldsBulder.put("servedVolumes", new SetFieldValueHandler<>(jacsStorageAgent.getServedVolumes()));
        updatedFieldsBulder.put("unavailableVolumeIds", new SetFieldValueHandler<>(jacsStorageAgent.getUnavailableVolumeIds()));
        jacsStorageAgentDao.update(jacsStorageAgent.getId(), updatedFieldsBulder.build());
    }

    public void updateAgentStatus(JacsStorageAgent jacsStorageAgent) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBulder = ImmutableMap.builder();
        if (StringUtils.isNotBlank(jacsStorageAgent.getStatus())) {
            updatedFieldsBulder.put("status", new SetFieldValueHandler<>(jacsStorageAgent.getStatus()));
        }
        jacsStorageAgentDao.update(jacsStorageAgent.getId(), updatedFieldsBulder.build());
    }

    public JacsStorageAgent getLocalStorageAgentInfo() {
        JacsStorageAgent storageAgent = jacsStorageAgentDao.findStorageAgentByHost(storageAgentId);
        Preconditions.checkState(storageAgent != null, "No storage agent found for " + storageAgentId);
        return storageAgent;
    }
}
