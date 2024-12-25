package org.janelia.jacsstorage.testrest;

public class TestResourceBinder {
    private final TestMasterStorageDependenciesProducer dependenciesProducer;

    public TestResourceBinder(TestMasterStorageDependenciesProducer dependenciesProducer) {
        this.dependenciesProducer = dependenciesProducer;
    }

//    @Override
//    protected void configure() {
//        Annotation remoteInstanceAnnotation;
//        try {
//            Method m = TestMasterStorageDependenciesProducer.class.getMethod("getStorageAllocatorService");
//            remoteInstanceAnnotation = m.getAnnotation(RemoteInstance.class);
//        } catch (NoSuchMethodException e) {
//            throw new IllegalStateException(e);
//        }
//        bind(dependenciesProducer.getStorageAgentManager()).to(StorageAgentManager.class);
//        bind(dependenciesProducer.getStorageAllocatorService()).qualifiedBy(remoteInstanceAnnotation).to(StorageAllocatorService.class);
//        bind(dependenciesProducer.getStorageLookupService()).qualifiedBy(remoteInstanceAnnotation).to(StorageLookupService.class);
//        bind(dependenciesProducer.getStorageUsageManager()).qualifiedBy(remoteInstanceAnnotation).to(StorageUsageManager.class);
//        bind(dependenciesProducer.getStorageVolumeManager()).qualifiedBy(remoteInstanceAnnotation).to(StorageVolumeManager.class);
//    }
}
