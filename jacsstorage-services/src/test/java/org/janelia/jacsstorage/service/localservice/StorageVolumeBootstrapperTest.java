package org.janelia.jacsstorage.service.localservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.hamcrest.Matcher;
import org.hamcrest.beans.HasPropertyWithValue;
import org.janelia.jacsstorage.cdi.ApplicationConfigProvider;
import org.janelia.jacsstorage.config.ApplicationConfig;
import org.janelia.jacsstorage.coreutils.NetUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StorageVolumeBootstrapperTest {

    private StorageVolumeManager storageVolumeManager;

    @Before
    public void setUp() {
        storageVolumeManager = mock(StorageVolumeManager.class);
        when(storageVolumeManager.updateVolumeInfo(any(JacsStorageVolume.class))).then(invocation -> invocation.getArgument(0));
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
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo("/" + NetUtils.getCurrentHostName() + "/jadestorage/${otherKey}/storage/${andAnother}")),
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
                                        .put("StorageVolume.v1.PathPrefix", "${StorageAgent.StorageHost}/jadestorage/${otherKey}/storage")
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
                                        new HasPropertyWithValue<>("storagePathPrefix", equalTo("/TheHost/jadestorage/${otherKey}/storage"))
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
            StorageVolumeBootstrapper storageVolumeBootstrapper = new StorageVolumeBootstrapper(
                    storageVolumeManager,
                    td.applicationConfig,
                    td.applicationConfig.getStringPropertyValue("StorageAgent.StorageHost"),
                    ImmutableList.of("v1", "v2")
            );
            List<JacsStorageVolume> storageVolumes = storageVolumeBootstrapper.initializeStorageVolumes();
            assertThat(storageVolumes, td.matcher);
        }
    }

}
