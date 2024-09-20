package org.janelia.jacsstorage.testrest;

import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.OriginalStorageContentReader;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class TestResourceBinder extends AbstractBinder {
    private final TestAgentStorageDependenciesProducer dependenciesProducer;

    public TestResourceBinder(TestAgentStorageDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

    @Override
    protected void configure() {
        Annotation localInstanceAnnotation;
        try {
            Method m = TestAgentStorageDependenciesProducer.class.getMethod("getStorageLookupService");
            localInstanceAnnotation = m.getAnnotation(LocalInstance.class);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
        bind(dependenciesProducer.getDataStorageService()).to(OriginalStorageContentReader.class);
        bind(dependenciesProducer.getDataStorageService()).to(DataStorageService.class);
        bind(dependenciesProducer.getStorageAllocatorService()).qualifiedBy(localInstanceAnnotation).to(StorageAllocatorService.class);
        bind(dependenciesProducer.getStorageLookupService()).qualifiedBy(localInstanceAnnotation).to(StorageLookupService.class);
        bind(dependenciesProducer.getStorageVolumeManager()).qualifiedBy(localInstanceAnnotation).to(StorageVolumeManager.class);
        bind(dependenciesProducer.getStorageUsageManager()).qualifiedBy(localInstanceAnnotation).to(StorageUsageManager.class);
        bind(dependenciesProducer.getAgentState()).to(AgentState.class);
    }
}
