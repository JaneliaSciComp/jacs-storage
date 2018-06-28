package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
        storageVolumeManager = new LocalStorageVolumeManager(storageVolumeDao, capacityNotifier, TEST_HOST);
    }

    @Test
    public void mangedVolumes() {
        Mockito.when(storageVolumeDao.findMatchingVolumes(any(StorageQuery.class), any(PageRequest.class)))
                .then(invocation -> new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .storageHost(TEST_HOST)
                                .storageRootDir("/root/testDir")
                                .addTag("t1").addTag("t2")
                                .storageServiceURL("http://storageURL")
                                .percentageFull(20)
                                .build(),
                        new JacsStorageVolumeBuilder()
                                .shared(true)
                                .storageRootDir("/root/testSharedDir")
                                .addTag("t1").addTag("t2")
                                .storageServiceURL("http://storageURL")
                                .percentageFull(20)
                                .build()
                        ))
                );
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery());
        verify(storageVolumeDao).findMatchingVolumes(
                argThat(argument -> TEST_HOST.equals(argument.getAccessibleOnHost())),
                any(PageRequest.class));
        assertThat(storageVolumes, hasSize(2));
    }

    @Test
    public void fullVolumeInfo() {
        String testVolumeName = "v1";
        Mockito.when(storageVolumeDao.findMatchingVolumes(any(StorageQuery.class), any(PageRequest.class)))
                .then(invocation -> new PageResult<>(invocation.getArgument(1), ImmutableList.of(
                        new JacsStorageVolumeBuilder()
                                .name(testVolumeName)
                                .storageHost(TEST_HOST)
                                .storageRootDir("/root/testDir")
                                .addTag("t1").addTag("t2")
                                .storageServiceURL("http://storageURL")
                                .percentageFull(20)
                                .build()
                        ))
                );
        prepareGetAvailableStorageBytes(199L, 300L);
        Optional<JacsStorageVolume> storageVolume = storageVolumeManager.getFullVolumeInfo(testVolumeName);
        verify(storageVolumeDao).findMatchingVolumes(
                argThat(argument -> testVolumeName.equals(argument.getStorageName()) && TEST_HOST.equals(argument.getAccessibleOnHost())),
                any(PageRequest.class));
        assertTrue(storageVolume.isPresent());
        storageVolume.ifPresent(sv -> {
            assertEquals(199L, sv.getAvailableSpaceInBytes().longValue());
        });
    }

    private void prepareGetAvailableStorageBytes(long usableSpace, long totalSpace) {
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
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void updateVolumeWhenIdIsNotKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageHost(testHost)
                .storageRootDir("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .percentageFull(20)
                .build();
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .name(testVolume.getName())
                .build();

        Mockito.when(storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName()))
                .then(invocation -> newlyCreatedTestVolume);
        prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(testVolume);

        Mockito.verify(storageVolumeDao).getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume, ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("storageRootDir", new SetFieldValueHandler<>(testVolume.getStorageRootDir()))
                .put("storagePathPrefix", new SetFieldValueHandler<>(testVolume.getStoragePathPrefix()))
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(testVolume.getAvailableSpaceInBytes()))
                .put("storageServiceURL", new SetFieldValueHandler<>(testVolume.getStorageServiceURL()))
                .put("percentageFull", new SetFieldValueHandler<>(testVolume.getPercentageFull()))
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
                .storageRootDir("/root/testDir")
                .storagePathPrefix("/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .availableSpace(200L)
                .percentageFull(70)
                .build();

        JacsStorageVolume updatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .storageRootDir("/root/testDir")
                .storagePathPrefix("/testDir")
                .addTag("t1").addTag("t2").addTag("t3")
                .storageServiceURL("http://storageURL")
                .availableSpace(100L)
                .percentageFull(90)
                .build();

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);
        prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(updatedTestVolume);

        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
        Mockito.verify(storageVolumeDao).update(testVolume, ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                .put("availableSpaceInBytes", new SetFieldValueHandler<>(updatedTestVolume.getAvailableSpaceInBytes()))
                .put("percentageFull", new SetFieldValueHandler<>(updatedTestVolume.getPercentageFull()))
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
                .storageRootDir("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .build();
        testVolume.setShared(true);
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .name(testVolume.getName())
                .build();
        newlyCreatedTestVolume.setShared(true);

        Mockito.when(storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName()))
                .then(invocation -> newlyCreatedTestVolume);
        prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(testVolume);

        Mockito.verify(storageVolumeDao).getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume, ImmutableMap.of(
                "storageRootDir", new SetFieldValueHandler<>(testVolume.getStorageRootDir()),
                "storagePathPrefix", new SetFieldValueHandler<>(testVolume.getStoragePathPrefix()),
                "storageTags", new SetFieldValueHandler<>(testVolume.getStorageTags())
        ));
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateSharedVolumeWhenIdIsKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .storageRootDir("/root/testDir")
                .storagePathPrefix("/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .build();
        testVolume.setShared(true);

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);
        prepareGetAvailableStorageBytes(199L, 300L);

        storageVolumeManager.updateVolumeInfo(testVolume);

        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

}
