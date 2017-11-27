package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matcher;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.AbstractStorageVolumeManager;
import org.janelia.jacsstorage.utils.NetUtils;
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

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LocalStorageVolumeManager.class})
public class LocalStorageVolumeManagerTest {

    private JacsStorageVolumeDao storageVolumeDao;

    @Before
    public void setUp() {
        storageVolumeDao = mock(JacsStorageVolumeDao.class);
    }

    @Test
    public void managedVolumes() {
        prepareGetAvailableStorageBytes();
        class TestData {
            private final ApplicationConfig applicationConfig;
            private final Matcher<Iterable<JacsStorageVolume>> matcher;

            public TestData(ApplicationConfig applicationConfig, Matcher<Iterable<JacsStorageVolume>> matcher) {
                this.applicationConfig = applicationConfig;
                this.matcher = matcher;
            }
        }
        TestData[] testData = new TestData[] {
                new TestData(
                        new ApplicationConfigProvider()
                                .fromMap(ImmutableMap.<String, String>builder()
                                        .put("StorageVolume.v1.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v1.PathPrefix", "${storageHost}/jadestorage/${otherKey}/storage/${andAnother}")
                                        .put("StorageVolume.v1.Shared", "false")
                                        .put("StorageVolume.v1.Tags", "local")
                                        .put("StorageVolume.v2.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v2.PathPrefix", "/shared/jadestorage")
                                        .put("StorageVolume.v2.Shared", "true")
                                        .put("StorageVolume.v2.Tags", "shared")
                                        .put("StorageAgent.StorageHost", "")
                                        .put("StorageVolume.OVERFLOW_VOLUME.RootDir", "/overflow")
                                        .build()
                                )
                                .build(),
                        hasItems(
                                allOf(new HasPropertyWithValue<>("name", equalTo("v1")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo(NetUtils.getCurrentHostName() + "/jadestorage/${otherKey}/storage/${andAnother}")),
                                        new HasPropertyWithValue<>("shared", equalTo(false))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("v2")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo("/shared/jadestorage")),
                                        new HasPropertyWithValue<>("shared", equalTo(true))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("OVERFLOW_VOLUME")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/overflow")),
                                        new HasPropertyWithValue<>("shared", equalTo(true))
                                )
                        )
                ),
                new TestData(
                        new ApplicationConfigProvider()
                                .fromMap(ImmutableMap.<String, String>builder()
                                        .put("StorageVolume.v1.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v1.PathPrefix", "${storageHost}/jadestorage/${otherKey}/storage")
                                        .put("StorageVolume.v1.Shared", "false")
                                        .put("StorageVolume.v1.Tags", "local")
                                        .put("StorageVolume.v2.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v2.PathPrefix", "/shared/jadestorage")
                                        .put("StorageVolume.v2.Shared", "true")
                                        .put("StorageVolume.v2.Tags", "shared")
                                        .put("StorageAgent.StorageHost", "TheHost")
                                        .put("StorageVolume.OVERFLOW_VOLUME.RootDir", "/overflow")
                                        .build()
                                )
                                .build(),
                        hasItems(
                                allOf(new HasPropertyWithValue<>("name", equalTo("v1")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo("TheHost/jadestorage/${otherKey}/storage"))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("v2")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo("/shared/jadestorage"))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("OVERFLOW_VOLUME")),
                                        new HasPropertyWithValue<>("storageRootDir", equalTo("/overflow"))
                                )
                        )
                )
        };
        for (TestData td : testData) {
            StorageVolumeManager storageVolumeManager = new LocalStorageVolumeManager(
                    storageVolumeDao,
                    td.applicationConfig,
                    td.applicationConfig.getStringPropertyValue("StorageAgent.StorageHost"),
                    ImmutableList.of("v1", "v2")
            );
            List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery());
            assertThat(storageVolumes, td.matcher);
        }
    }

    private void prepareGetAvailableStorageBytes() {
        try {
            FileStore testFileStore = mock(FileStore.class);
            PowerMockito.mockStatic(Files.class);
            Mockito.when(Files.notExists(any(Path.class)))
                    .thenReturn(true, false, false);
            Mockito.when(Files.createDirectories(any(Path.class)))
                    .then((Answer<Path>) invocation -> invocation.getArgument(0));
            Mockito.when(Files.getFileStore(any(Path.class)))
                    .then((Answer<FileStore>) invocation -> testFileStore);
            Mockito.when(testFileStore.getUsableSpace()).thenReturn(199L);
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
                .tcpPortNo(100)
                .build();
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .name(testVolume.getName())
                .build();
        ApplicationConfig applicationConfig = mock(ApplicationConfig.class);

        prepareGetAvailableStorageBytes();

        Mockito.when(storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName()))
                .then(invocation -> newlyCreatedTestVolume);
        StorageVolumeManager storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                applicationConfig,
                testHost,
                ImmutableList.of("storageVol")
        );
        storageVolumeManager.updateVolumeInfo(testVolume);
        Mockito.verify(storageVolumeDao).getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume, ImmutableMap.of(
                "storageRootDir", new SetFieldValueHandler<>(testVolume.getStorageRootDir()),
                "availableSpaceInBytes", new SetFieldValueHandler<>(testVolume.getAvailableSpaceInBytes()),
                "storageServiceURL", new SetFieldValueHandler<>(testVolume.getStorageServiceURL()),
                "storageServiceTCPPortNo", new SetFieldValueHandler<>(testVolume.getStorageServiceTCPPortNo()),
                "volumeTags", new SetFieldValueHandler<>(testVolume.getStorageTags())
        ));
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }

    @Test
    public void updateVolumeWhenIdIsKnown() {
        final String testHost = "TheTestHost";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .storageRootDir("/root/testDir")
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .tcpPortNo(100)
                .build();
        ApplicationConfig applicationConfig = mock(ApplicationConfig.class);

        prepareGetAvailableStorageBytes();

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);

        StorageVolumeManager storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                applicationConfig,
                testHost,
                ImmutableList.of("storageVol")
        );
        storageVolumeManager.updateVolumeInfo(testVolume);
        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
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
                .tcpPortNo(100)
                .build();
        testVolume.setShared(true);
        JacsStorageVolume newlyCreatedTestVolume = new JacsStorageVolumeBuilder()
                .storageVolumeId(1L)
                .storageHost(testHost)
                .name(testVolume.getName())
                .build();
        ApplicationConfig applicationConfig = mock(ApplicationConfig.class);

        prepareGetAvailableStorageBytes();

        Mockito.when(storageVolumeDao.getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName()))
                .then(invocation -> newlyCreatedTestVolume);
        StorageVolumeManager storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                applicationConfig,
                testHost,
                ImmutableList.of("storageVol")
        );
        storageVolumeManager.updateVolumeInfo(testVolume);
        Mockito.verify(storageVolumeDao).getStorageByHostAndNameAndCreateIfNotFound(testHost, testVolume.getName());
        Mockito.verify(storageVolumeDao).update(newlyCreatedTestVolume, ImmutableMap.of(
                "storageRootDir", new SetFieldValueHandler<>(testVolume.getStorageRootDir()),
                "volumeTags", new SetFieldValueHandler<>(testVolume.getStorageTags())
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
                .addTag("t1").addTag("t2")
                .storageServiceURL("http://storageURL")
                .tcpPortNo(100)
                .build();
        testVolume.setShared(true);
        ApplicationConfig applicationConfig = mock(ApplicationConfig.class);

        prepareGetAvailableStorageBytes();

        Mockito.when(storageVolumeDao.findById(testVolume.getId())).thenReturn(testVolume);

        StorageVolumeManager storageVolumeManager = new LocalStorageVolumeManager(
                storageVolumeDao,
                applicationConfig,
                testHost,
                ImmutableList.of("storageVol")
        );
        storageVolumeManager.updateVolumeInfo(testVolume);
        Mockito.verify(storageVolumeDao).findById(testVolume.getId());
        Mockito.verifyNoMoreInteractions(storageVolumeDao);
    }
}
