package org.janelia.jacsstorage.service.impl.localservice;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageAgent;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.AgentStatePersistence;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageCapacity;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class LocalStorageVolumeManagerTest {
    private static final String TEST_HOST = "testHost";

    private JacsStorageVolumeDao storageVolumeDao;
    private DataContentService dataContentService;
    private AgentStatePersistence agentStatePersistence;
    private NotificationService capacityNotifier;
    private StorageVolumeManager storageVolumeManager;

    @Before
    public void setUp() {
        storageVolumeDao = mock(JacsStorageVolumeDao.class);
        dataContentService = mock(DataContentService.class);
        capacityNotifier = mock(NotificationService.class);
        agentStatePersistence = mock(AgentStatePersistence.class);
        Mockito.when(agentStatePersistence.getLocalStorageAgentInfo())
                .then(invocation -> {
                    JacsStorageAgent jacsStorageAgent = new JacsStorageAgent();
                    jacsStorageAgent.setAgentHost(TEST_HOST);
                    jacsStorageAgent.setServedVolumes(ImmutableSet.of("v1"));
                    return jacsStorageAgent;
                });
        storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                dataContentService,
                agentStatePersistence,
                capacityNotifier);
    }

    @Test
    public void mangedVolumes() {
        Mockito.when(dataContentService.exists(JADEStorageURI.createStoragePathURI("/root/testDir", new JADEStorageOptions()))).thenReturn(true);
        Mockito.when(dataContentService.exists(JADEStorageURI.createStoragePathURI("/root/testSharedDir", new JADEStorageOptions()))).thenReturn(true);
        Mockito.when(dataContentService.exists(JADEStorageURI.createStoragePathURI("/root/notAccessible", new JADEStorageOptions()))).thenReturn(false);

        Mockito.when(storageVolumeDao.findMatchingVolumes(any(StorageQuery.class), any(PageRequest.class)))
                .then(invocation -> {
                            StorageQuery q = invocation.getArgument(0);
                            if (TEST_HOST.equals(q.getAccessibleOnAgent())) {
                                return new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                                        new JacsStorageVolumeBuilder()
                                                .name("v1")
                                                .storageAgentId(TEST_HOST)
                                                .storageRootTemplate("/root/testDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .active(true)
                                                .percentageFull(20)
                                                .build()
                                ));
                            } else {
                                return new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                                        new JacsStorageVolumeBuilder()
                                                .name("v1")
                                                .storageAgentId(TEST_HOST)
                                                .storageRootTemplate("/root/testDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
                                                .active(true)
                                                .build(),
                                        new JacsStorageVolumeBuilder()
                                                .name("v1")
                                                .shared(true)
                                                .storageRootTemplate("/root/testSharedDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
                                                .active(true)
                                                .build(),
                                        new JacsStorageVolumeBuilder()
                                                .name("v1")
                                                .storageAgentId(TEST_HOST)
                                                .storageRootTemplate("/root/notAccessible")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
                                                .active(true)
                                                .build()
                                ));
                            }
                        }
                );
        StorageQuery storageQuery = new StorageQuery();
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(storageQuery);
        verify(storageVolumeDao).findMatchingVolumes(
                eq(storageQuery),
                any(PageRequest.class));
        assertThat(storageVolumes, hasSize(2));
    }

    private static class TestDiskUsage {
        private final long usableSpace;
        private final long totalSpace;
        private final int usagePercentage;

        TestDiskUsage(long usableSpace, long totalSpace) {
            this.usableSpace = usableSpace;
            this.totalSpace = totalSpace;
            if (totalSpace != 0) {
                usagePercentage = (int) ((totalSpace - usableSpace) * 100 / totalSpace);
            } else {
                usagePercentage = 0;
            }
        }
    }

    private TestDiskUsage prepareGetAvailableStorageBytes(long usableSpace, long totalSpace) {
        Mockito.when(dataContentService.storageCapacity(any())).thenReturn(new StorageCapacity(totalSpace, usableSpace));
        return new TestDiskUsage(usableSpace, totalSpace);
    }

    @Test
    public void updateVolumeWhenExistingVolumeDoesNotHaveRootStorageSet() {
        JacsStorageVolume existingVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageAgentId(TEST_HOST)
                .name("v1")
                .active(true)
                .build();
        JacsStorageVolume updatedVolumeFields = new JacsStorageVolumeBuilder()
                .storageRootTemplate("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .percentageFull(20)
                .active(true)
                .build();

        Mockito.when(storageVolumeDao.findById(existingVolume.getId())).thenReturn(existingVolume);
        Mockito.when(storageVolumeDao.update(eq(existingVolume.getId()), anyMap())).thenReturn(existingVolume);
        Mockito.when(dataContentService.exists(JADEStorageURI.createStoragePathURI("/root/testDir", new JADEStorageOptions()))).thenReturn(true);

        TestDiskUsage diskUsage = prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(existingVolume.getId(), updatedVolumeFields);

        Mockito.verify(storageVolumeDao).findById(existingVolume.getId());
        Mockito.verify(storageVolumeDao).update(existingVolume.getId(), ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("storageRootTemplate", new SetFieldValueHandler<>(updatedVolumeFields.getStorageRootTemplate()))
                .put("storageVirtualPath", new SetFieldValueHandler<>(updatedVolumeFields.getStorageVirtualPath()))
                .put("storageServiceURL", new SetFieldValueHandler<>(updatedVolumeFields.getStorageServiceURL()))
                .put("storageTags", new SetFieldValueHandler<>(updatedVolumeFields.getStorageTags()))
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(diskUsage.usableSpace))
                .put("percentageFull", new SetFieldValueHandler<>(diskUsage.usagePercentage))
                .build()
        );
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateVolumeWhenExistingRootStorageIsSet() {
        JacsStorageVolume existingVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .name("v1")
                .storageAgentId(TEST_HOST)
                .storageRootTemplate("/root/testDir")
                .storageVirtualPath("/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .availableSpace(200L)
                .percentageFull(70)
                .active(true)
                .build();

        JacsStorageVolume updatedTestVolumeFields = new JacsStorageVolumeBuilder()
                .storageRootTemplate("/root/testDir")
                .storageVirtualPath("/testDir")
                .addTag("t1").addTag("t2").addTag("t3")
                .storageServiceURL("http://storageURL")
                .active(true)
                .build();

        Mockito.when(storageVolumeDao.findById(existingVolume.getId())).thenReturn(existingVolume);
        Mockito.when(storageVolumeDao.update(eq(existingVolume.getId()), anyMap())).thenReturn(existingVolume);
        Mockito.when(dataContentService.exists(JADEStorageURI.createStoragePathURI("/root/testDir", new JADEStorageOptions()))).thenReturn(true);
        TestDiskUsage diskUsage = prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(existingVolume.getId(), updatedTestVolumeFields);

        Mockito.verify(storageVolumeDao).findById(existingVolume.getId());
        Mockito.verify(storageVolumeDao).update(existingVolume.getId(), ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("storageTags", new SetFieldValueHandler<>(updatedTestVolumeFields.getStorageTags()))
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(diskUsage.usableSpace))
                .put("percentageFull", new SetFieldValueHandler<>(diskUsage.usagePercentage))
                .build()
        );
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateSharedVolumeWhenIdIsNotKnown() {
        final String testAgentId = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageAgentId(testAgentId)
                .storageRootTemplate("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .build();
        testVolume.setShared(true);
        TestDiskUsage diskUsage = new TestDiskUsage(199L, 300L);
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageAgentId(testAgentId)
                .availableSpace(diskUsage.usableSpace)
                .percentageFull(diskUsage.usagePercentage)
                .name(testVolume.getName())
                .build();
        newlyCreatedTestVolume.setShared(true);

        Mockito.when(storageVolumeDao.findById(newlyCreatedTestVolume.getId())).thenReturn(newlyCreatedTestVolume);
        Mockito.when(storageVolumeDao.update(eq(newlyCreatedTestVolume.getId()), anyMap())).thenReturn(newlyCreatedTestVolume);
        prepareGetAvailableStorageBytes(diskUsage.usableSpace, diskUsage.totalSpace);

        storageVolumeManager.updateVolumeInfo(newlyCreatedTestVolume.getId(), testVolume);

        Mockito.verify(storageVolumeDao).findById(newlyCreatedTestVolume.getId());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume.getId(), ImmutableMap.of(
                "storageRootTemplate", new SetFieldValueHandler<>(testVolume.getStorageRootTemplate()),
                "storageVirtualPath", new SetFieldValueHandler<>(testVolume.getStorageVirtualPath()),
                "storageTags", new SetFieldValueHandler<>(testVolume.getStorageTags())
        ));
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateSharedVolumeWhenIdIsKnown() {
        final String testAgentId = "TheTestHost";
        TestDiskUsage diskUsage = new TestDiskUsage(199L, 300L);
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .name("v1")
                .storageAgentId(testAgentId)
                .storageRootTemplate("/root/testDir")
                .storageVirtualPath("/testDir")
                .availableSpace(diskUsage.usableSpace)
                .percentageFull(diskUsage.usagePercentage)
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .build();
        testVolume.setShared(true);

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);
        prepareGetAvailableStorageBytes(diskUsage.usableSpace, diskUsage.totalSpace);
        storageVolumeManager.updateVolumeInfo(testVolume.getId(), testVolume);

        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

}
