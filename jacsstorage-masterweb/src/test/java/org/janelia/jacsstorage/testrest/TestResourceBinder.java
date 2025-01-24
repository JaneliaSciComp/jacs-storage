package org.janelia.jacsstorage.testrest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.impl.distributedservice.StorageAgentManager;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class TestResourceBinder extends AbstractBinder {
    private final TestMasterStorageDependenciesProducer dependenciesProducer;

    public TestResourceBinder(TestMasterStorageDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        Annotation remoteInstanceAnnotation;
        try {
            Method m = TestMasterStorageDependenciesProducer.class.getMethod("getStorageAllocatorService");
            remoteInstanceAnnotation = m.getAnnotation(RemoteInstance.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
        bind(dependenciesProducer.getStorageAgentManager()).to(StorageAgentManager.class);
        bind(dependenciesProducer.getStorageAllocatorService()).qualifiedBy(remoteInstanceAnnotation).to(StorageAllocatorService.class);
        bind(dependenciesProducer.getStorageLookupService()).qualifiedBy(remoteInstanceAnnotation).to(StorageLookupService.class);
        bind(dependenciesProducer.getStorageUsageManager()).qualifiedBy(remoteInstanceAnnotation).to(StorageUsageManager.class);
        bind(dependenciesProducer.getStorageVolumeManager()).qualifiedBy(remoteInstanceAnnotation).to(StorageVolumeManager.class);
    }
}
