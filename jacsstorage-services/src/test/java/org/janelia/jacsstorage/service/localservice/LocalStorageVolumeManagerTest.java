package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalStorageVolumeManager.class})
public class LocalStorageVolumeManagerTest {
    private static final String TEST_HOST = "testHost";

    private JacsStorageVolumeDao storageVolumeDao;
    private NotificationService capacityNotifier;
    private StorageVolumeManager storageVolumeManager;

    @Before
    public void setUp() {
        storageVolumeDao = mock(JacsStorageVolumeDao.class);
        capacityNotifier = mock(NotificationService.class);
        storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                capacityNotifier);
    }

    @Test
    public void mangedVolumes() {
        Mockito.when(storageVolumeDao.findMatchingVolumes(any(StorageQuery.class), any(PageRequest.class)))
                .then(invocation -> {
                            StorageQuery q = invocation.getArgument(0);
                            if (TEST_HOST.equals(q.getAccessibleOnHost())) {
                                return new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                                        new JacsStorageVolumeBuilder()
                                                .storageHost(TEST_HOST)
                                                .storageRootTemplate("/root/testDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
                                                .build()
                                ));
                            } else {
                                return new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                                        new JacsStorageVolumeBuilder()
                                                .storageHost(TEST_HOST)
                                                .storageRootTemplate("/root/testDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
                                                .build(),
                                        new JacsStorageVolumeBuilder()
                                                .shared(true)
                                                .storageRootTemplate("/root/testSharedDir")
                                                .addTag("t1").addTag("t2")
                                                .storageServiceURL("http://storageURL")
                                                .percentageFull(20)
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
        try {
            FileStore testFileStore = mock(FileStore.class);
            PowerMockito.mockStatic(Files.class);
            Mockito.when(Files.notExists(any(Path.class)))
                    .thenReturn(true, false, false);
            Mockito.when(Files.createDirectories(any(Path.class)))
                    .then((Answer<Path>) invocation -> invocation.getArgument(0));
            Mockito.when(Files.getFileStore(any(Path.class)))
                    .then((Answer<FileStore>) invocation -> testFileStore);
            Mockito.when(testFileStore.getUsableSpace()).thenReturn(usableSpace);
            Mockito.when(testFileStore.getTotalSpace()).thenReturn(totalSpace);
            return new TestDiskUsage(usableSpace, totalSpace);
        } catch (Exception e) {
            fail(e.toString());
            return null;
        }
    }

    @Test
    public void updateVolumeWhenIdIsNotKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageHost(testHost)
                .storageRootTemplate("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .percentageFull(20)
                .build();
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .name(testVolume.getName())
                .build();

        Mockito.when(storageVolumeDao.createStorageVolumeIfNotFound(testHost, testVolume.getName()))
                .then(invocation -> newlyCreatedTestVolume);
        Mockito.when(storageVolumeDao.findById(newlyCreatedTestVolume.getId()))
                .thenReturn(newlyCreatedTestVolume);

        TestDiskUsage diskUsage = prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(newlyCreatedTestVolume.getId(), testVolume);

        Mockito.verify(storageVolumeDao).findById(newlyCreatedTestVolume.getId());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume.getId(), ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("storageRootTemplate", new SetFieldValueHandler<>(testVolume.getStorageRootTemplate()))
                .put("storageVirtualPath", new SetFieldValueHandler<>(testVolume.getStorageVirtualPath()))
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(diskUsage.usableSpace))
                .put("storageServiceURL", new SetFieldValueHandler<>(testVolume.getStorageServiceURL()))
                .put("percentageFull", new SetFieldValueHandler<>(diskUsage.usagePercentage))
                .put("storageTags", new SetFieldValueHandler<>(testVolume.getStorageTags()))
                .build()
        );
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateVolumeWhenIdIsKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .storageRootTemplate("/root/testDir")
                .storageVirtualPath("/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .availableSpace(200L)
                .percentageFull(70)
                .build();

        JacsStorageVolume updatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .storageRootTemplate("/root/testDir")
                .storageVirtualPath("/testDir")
                .addTag("t1").addTag("t2").addTag("t3")
                .storageServiceURL("http://storageURL")
                .build();

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);
        TestDiskUsage diskUsage = prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(updatedTestVolume.getId(), updatedTestVolume);

        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
        Mockito.verify(storageVolumeDao).update(testVolume.getId(), ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(diskUsage.usableSpace))
                .put("percentageFull", new SetFieldValueHandler<>(diskUsage.usagePercentage))
                .put("storageTags", new SetFieldValueHandler<>(updatedTestVolume.getStorageTags()))
                .build()
        );
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateSharedVolumeWhenIdIsNotKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageHost(testHost)
                .storageRootTemplate("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .build();
        testVolume.setShared(true);
        TestDiskUsage diskUsage = new TestDiskUsage(199L, 300L);
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .availableSpace(diskUsage.usableSpace)
                .percentageFull(diskUsage.usagePercentage)
                .name(testVolume.getName())
                .build();
        newlyCreatedTestVolume.setShared(true);

        Mockito.when(storageVolumeDao.findById(newlyCreatedTestVolume.getId())).thenReturn(newlyCreatedTestVolume);
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
        final String testHost = "TheTestHost";
        TestDiskUsage diskUsage = new TestDiskUsage(199L, 300L);
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
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
