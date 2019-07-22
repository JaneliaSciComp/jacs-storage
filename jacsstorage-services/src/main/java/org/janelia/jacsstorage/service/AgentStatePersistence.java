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
    private final String storageAgentHost;

    @Inject
    public AgentStatePersistence(JacsStorageAgentDao jacsStorageAgentDao,
                                 @PropertyValue(name = "StorageAgent.StorageHost") String storageHost) {
        this.jacsStorageAgentDao = jacsStorageAgentDao;
        this.storageAgentHost = StringUtils.defaultIfBlank(storageHost, NetUtils.getCurrentHostName());
    }

    public JacsStorageAgent createAgentStorage(String agentHost, String agentAccessURL, String status) {
        return jacsStorageAgentDao.createStorageAgentIfNotFound(agentHost, agentAccessURL, status, ImmutableSet.of("*"));
    }

    public JacsStorageAgent updateAgentServedVolumes(Number agentStorageId, Set<String> servedVolumes) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBulder = ImmutableMap.builder();
        if (CollectionUtils.isNotEmpty(servedVolumes)) {
            updatedFieldsBulder.put("servedVolumes", new SetFieldValueHandler<>(servedVolumes));
        }
        return jacsStorageAgentDao.update(agentStorageId, updatedFieldsBulder.build());
    }

    public JacsStorageAgent updateAgentStatus(Number agentStorageId, String status) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBulder = ImmutableMap.builder();
        if (StringUtils.isNotBlank(status)) {
            updatedFieldsBulder.put("status", new SetFieldValueHandler<>(status));
        }
        return jacsStorageAgentDao.update(agentStorageId, updatedFieldsBulder.build());
    }

    public JacsStorageAgent getLocalStorageAgentInfo() {
        JacsStorageAgent storageAgent = jacsStorageAgentDao.findStorageAgentByHost(storageAgentHost);
        Preconditions.checkState(storageAgent != null, "No storage agent found for " + storageAgentHost);
        return storageAgent;
    }
}
