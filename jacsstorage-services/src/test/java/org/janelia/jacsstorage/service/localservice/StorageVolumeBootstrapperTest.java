package org.janelia.jacsstorage.service.localservice;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageVolumeBootstrapperTest {

    private static final Long TEST_VOLUME_ID = 10L;
    private static final String TEST_STORAGE_HOST = "TestHost";
    private static final String TEST_STORAGE_PORT = "8881";

    private StorageVolumeManager storageVolumeManager;

    @Before
    public void setUp() {
        storageVolumeManager = mock(StorageVolumeManager.class);
        Mockito.when(storageVolumeManager.createStorageVolumeIfNotFound(anyString(), argThat(argument -> true)))
                .then(invocation -> {
                    String volumeName = invocation.getArgument(0);
                    String storageAgentId = invocation.getArgument(1);
                    JacsStorageVolume sv = new JacsStorageVolume();
                    sv.setId(TEST_VOLUME_ID);
                    sv.setName(volumeName);
                    sv.setStorageAgentId(storageAgentId);
                    sv.setShared(StringUtils.isBlank(storageAgentId));
                    return sv;
                });
        when(storageVolumeManager.updateVolumeInfo(any(Number.class), any(JacsStorageVolume.class))).then(invocation -> invocation.getArgument(1));
    }

    @Test
    public void initializeStorageVolumes() {
        class TestData {
            private final ApplicationConfig applicationConfig;
            private final Matcher<Iterable<JacsStorageVolume>> matcher;

            private TestData(ApplicationConfig applicationConfig, Matcher<Iterable<JacsStorageVolume>> matcher) {
                this.applicationConfig = applicationConfig;
                this.matcher = matcher;
            }
        }
        @SuppressWarnings("unchecked")
        TestData[] testData = new TestData[] {
                new TestData(
                        new ApplicationConfigProvider()
                                .fromMap(ImmutableMap.<String, String>builder()
                                        .put("StorageVolume.v1.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v1.VirtualPath", "${storageHost}/jadestorage/${otherKey}/storage/${andAnother}")
                                        .put("StorageVolume.v1.Shared", "false")
                                        .put("StorageVolume.v1.Tags", "local")
                                        .put("StorageVolume.v2.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v2.VirtualPath", "/shared/jadestorage")
                                        .put("StorageVolume.v2.Shared", "true")
                                        .put("StorageVolume.v2.Tags", "shared")
                                        .put("StorageAgent.StorageHost", "")
                                        .put("otherKey", "otherKeyValue")
                                        .put("andAnother", "andAnotherValue")
                                        .put("StorageVolume.OVERFLOW_VOLUME.RootDir", "/overflow")
                                        .build()
                                )
                                .build(),
                        hasItems(
                                allOf(new HasPropertyWithValue<>("name", equalTo("v1")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storageVirtualPath", equalTo("/" + NetUtils.getCurrentHostName() + "_" + TEST_STORAGE_PORT + "/jadestorage/otherKeyValue/storage/andAnotherValue")),
                                        new HasPropertyWithValue<>("shared", equalTo(false))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("v2")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storageVirtualPath", equalTo("/shared/jadestorage")),
                                        new HasPropertyWithValue<>("shared", equalTo(true))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("OVERFLOW_VOLUME")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/overflow")),
                                        new HasPropertyWithValue<>("shared", equalTo(true))
                                )
                        )
                ),
                new TestData(
                        new ApplicationConfigProvider()
                                .fromMap(ImmutableMap.<String, String>builder()
                                        .put("StorageVolume.v1.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v1.VirtualPath", "${StorageAgent.StorageHost}/jadestorage/${otherKey}/storage")
                                        .put("StorageVolume.v1.Shared", "false")
                                        .put("StorageVolume.v1.Tags", "local")
                                        .put("StorageVolume.v2.RootDir", "/data/jadestorage")
                                        .put("StorageVolume.v2.VirtualPath", "/shared/jadestorage")
                                        .put("StorageVolume.v2.Shared", "true")
                                        .put("StorageVolume.v2.Tags", "shared")
                                        .put("StorageAgent.StorageHost", "TheHost")
                                        .put("StorageVolume.OVERFLOW_VOLUME.RootDir", "/overflow")
                                        .build()
                                )
                                .build(),
                        hasItems(
                                allOf(new HasPropertyWithValue<>("name", equalTo("v1")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storageVirtualPath", equalTo("/TheHost/jadestorage/${otherKey}/storage"))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("v2")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/data/jadestorage")),
                                        new HasPropertyWithValue<>("storageVirtualPath", equalTo("/shared/jadestorage"))
                                ),
                                allOf(new HasPropertyWithValue<>("name", equalTo("OVERFLOW_VOLUME")),
                                        new HasPropertyWithValue<>("storageRootTemplate", equalTo("/overflow"))
                                )
                        )
                )
        };
        for (int ti = 0; ti < testData.length; ti++) {
            TestData td = testData[ti];
            StorageVolumeBootstrapper storageVolumeBootstrapper = new StorageVolumeBootstrapper(
                    storageVolumeManager,
                    td.applicationConfig,
                    td.applicationConfig.getStringPropertyValue("StorageAgent.StorageHost", NetUtils.getCurrentHostName()),
                    TEST_STORAGE_PORT,
                    ImmutableList.of("v1", "v2")
            );
            List<JacsStorageVolume> storageVolumes = storageVolumeBootstrapper.initializeStorageVolumes(TEST_STORAGE_HOST);
            assertThat("Test "+ ti, storageVolumes, td.matcher);
        }
    }

}
