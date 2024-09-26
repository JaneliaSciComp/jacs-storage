package org.janelia.jacsstorage.testrest;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.janelia.jacsstorage.cdi.ObjectMapperFactory;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.NumberSerializerModule;
import org.janelia.jacsstorage.filter.AuthFilter;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.impl.distributedservice.StorageAgentManager;

import javax.enterprise.inject.Produces;

import static org.mockito.Mockito.mock;

public class TestMasterStorageDependenciesProducer {

    private StorageAgentManager storageAgentManager = mock(StorageAgentManager.class);
    private StorageAllocatorService storageAllocatorService = mock(StorageAllocatorService.class);
    private StorageLookupService storageLookupService = mock(StorageLookupService.class);
    private StorageUsageManager storageUsageManager = mock(StorageUsageManager.class);
    private StorageVolumeManager storageVolumeManager = mock(StorageVolumeManager.class);
    private AuthFilter authFilter = mock(AuthFilter.class);

    @Produces
    public StorageAgentManager getStorageAgentManager() {
        return storageAgentManager;
    }

    @Produces @RemoteInstance
    public StorageAllocatorService getStorageAllocatorService() {
        return storageAllocatorService;
    }

    @Produces @RemoteInstance
    public StorageLookupService getStorageLookupService() {
        return storageLookupService;
    }

    @Produces @RemoteInstance
    public StorageUsageManager getStorageUsageManager() {
        return storageUsageManager;
    }

    @Produces @RemoteInstance
    public StorageVolumeManager getStorageVolumeManager() {
        return storageVolumeManager;
    }

    @Produces
    public ObjectMapperFactory getObjectMapperFactory() {
        return ObjectMapperFactory.instance();
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
