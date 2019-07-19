package org.janelia.jacsstorage.service;

import java.util.Date;
import java.util.Set;

import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsStorageAgentDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;

public class AgentStatePersistence {

    private final JacsStorageAgentDao jacsStorageAgentDao;

    @Inject
    public AgentStatePersistence(JacsStorageAgentDao jacsStorageAgentDao) {
        this.jacsStorageAgentDao = jacsStorageAgentDao;
    }

    public JacsStorageAgent createAgentStorage(String agentHost, String agentAccessURL, String status) {
        return jacsStorageAgentDao.createStorageAgentIfNotFound(agentHost, agentAccessURL, status, ImmutableSet.of("*"));
    }

    public JacsStorageAgent updateAgentStorage(Number agentStorageId, String status, Set<String> servedVolumes) {
        ImmutableMap.Builder<String, EntityFieldValueHandler<?>> updatedFieldsBulder = ImmutableMap.builder();
        if (StringUtils.isNotBlank(status)) {
            updatedFieldsBulder.put("status", new SetFieldValueHandler<>(status));
            updatedFieldsBulder.put("lastStatusCheck", new SetFieldValueHandler<>(new Date()));
        }
        if (CollectionUtils.isNotEmpty(servedVolumes)) {
            updatedFieldsBulder.put("servedVolumes", new SetFieldValueHandler<>(servedVolumes));
        }
        return jacsStorageAgentDao.update(agentStorageId, updatedFieldsBulder.build());
    }
}
