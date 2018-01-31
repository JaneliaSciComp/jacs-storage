package org.janelia.jacsstorage.testrest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.filter.JWTAuthFilter;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;

import javax.enterprise.inject.Produces;

import static org.mockito.Mockito.mock;

public class TestAgentStorageDependenciesProducer {

    private DataStorageService dataStorageService = mock(DataStorageService.class);
    private StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
    private StorageLookupService storageLookupService = mock(StorageLookupService.class);
    private StorageVolumeManager storageVolumeManager = mock(StorageVolumeManager.class);
    private AgentState agentState = mock(AgentState.class);
    private JWTAuthFilter jwtAuthFilter = mock(JWTAuthFilter.class);

    @Produces
    public DataStorageService getDataStorageService() {
        return dataStorageService;
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
    public StorageVolumeManager getStorageVolumeManager() {
        return storageVolumeManager;
    }

    @Produces
    public AgentState getAgentState() {
        return agentState;
    }

    @Produces
    public ObjectMapper getObjectMapper() {
        return new ObjectMapper();
    }

    @Produces
    public JWTAuthFilter getJwtAuthFilter() {
        return jwtAuthFilter;
    }

}
