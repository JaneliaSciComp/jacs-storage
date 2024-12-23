package org.janelia.jacsstorage.testrest;

import jakarta.enterprise.inject.Produces;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.NumberSerializerModule;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.n5.N5ContentService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;

import static org.mockito.Mockito.mock;

public class TestAgentStorageDependenciesProducer {

    private DataContentService dataContentService = mock(DataContentService.class);
    private N5ContentService n5ContentService = mock(N5ContentService.class);
    private StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
    private StorageLookupService storageLookupService = mock(StorageLookupService.class);
    private StorageUsageManager storageUsageManager = mock(StorageUsageManager.class);
    private StorageVolumeManager storageVolumeManager = mock(StorageVolumeManager.class);
    private AgentState agentState = mock(AgentState.class);
    private AuthFilter authFilter = mock(AuthFilter.class);

    @Produces
    public DataContentService getDataContentService() {
        return dataContentService;
    }

    @Produces
    public N5ContentService getN5ContentService() {
        return n5ContentService;
    }

    @Produces @LocalInstance
    public StorageAllocatorService getStorageAllocatorService() {
        return storageAllocatorService;
    }

    @Produces @LocalInstance
    public StorageLookupService getStorageLookupService() {
        return storageLookupService;
    }

    @Produces @LocalInstance
    public StorageUsageManager getStorageUsageManager() {
        return storageUsageManager;
    }

    @Produces @LocalInstance
    public StorageVolumeManager getStorageVolumeManager() {
        return storageVolumeManager;
    }

    @Produces
    public AgentState getAgentState() {
        return agentState;
    }

    @Produces
    public ObjectMapperFactory getObjectMapperFactory() {
        return new ObjectMapperFactory();
    }

    @Produces
    public ObjectMapper getObjectMapper() {
        return getObjectMapperFactory().newObjectMapper().registerModule(new NumberSerializerModule());
    }

    @Produces
    public AuthFilter getAuthFilter() {
        return authFilter;
    }

}
