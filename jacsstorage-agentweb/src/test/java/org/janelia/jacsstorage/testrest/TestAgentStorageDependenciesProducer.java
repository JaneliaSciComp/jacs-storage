package org.janelia.jacsstorage.testrest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Default;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.NumberSerializerModule;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.n5.N5ContentService;

import static org.mockito.Mockito.mock;

@ApplicationScoped
public class TestAgentStorageDependenciesProducer {

    private static DataContentService dataContentService = mock(DataContentService.class);
    private static N5ContentService n5ContentService = mock(N5ContentService.class);
    private static StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
    private static StorageLookupService storageLookupService = mock(StorageLookupService.class);
    private static StorageUsageManager storageUsageManager = mock(StorageUsageManager.class);
    private static StorageVolumeManager storageVolumeManager = mock(StorageVolumeManager.class);
    private static AgentState agentState = mock(AgentState.class);
    private static AuthFilter authFilter = mock(AuthFilter.class);

    @Produces @ApplicationScoped
    public DataContentService getDataContentService() {
        return dataContentService;
    }

    @Produces @ApplicationScoped
    public N5ContentService getN5ContentService() {
        return n5ContentService;
    }

    @Produces @LocalInstance @ApplicationScoped
    public StorageAllocatorService getStorageAllocatorService() {
        return storageAllocatorService;
    }

    @Produces @LocalInstance @Singleton
    public StorageLookupService getStorageLookupService() {
        return storageLookupService;
    }

    @Produces @LocalInstance @ApplicationScoped
    public StorageUsageManager getStorageUsageManager() {
        return storageUsageManager;
    }

    @Produces @LocalInstance @ApplicationScoped
    public StorageVolumeManager getStorageVolumeManager() {
        return storageVolumeManager;
    }

    @Produces @ApplicationScoped
    public AgentState getAgentState() {
        return agentState;
    }

    @Produces @ApplicationScoped
    public ObjectMapperFactory getObjectMapperFactory() {
        return new ObjectMapperFactory();
    }

    @Produces @Default
    public ObjectMapper getObjectMapper() {
        return getObjectMapperFactory().newObjectMapper().registerModule(new NumberSerializerModule());
    }

    @Produces @Default
    public AuthFilter getAuthFilter() {
        return authFilter;
    }

}
