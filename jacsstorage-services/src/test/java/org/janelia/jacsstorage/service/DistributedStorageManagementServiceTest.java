package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DistributedStorageManagementServiceTest {

    private StorageAgentManager storageAgentManager;
    private JacsStorageVolumeDao storageVolumeDao;
    private JacsBundleDao bundleDao;
    private DistributedStorageManagementService testStorageManagementService;

    @Before
    public void setUp() {
        storageAgentManager = mock(StorageAgentManager.class);
        storageVolumeDao = mock(JacsStorageVolumeDao.class);
        bundleDao = mock(JacsBundleDao.class);
        testStorageManagementService = new DistributedStorageManagementService(storageAgentManager, storageVolumeDao, bundleDao);
    }

    @Test
    public void useBadIdToGetBundle() {
        Long badId = 0L;
        JacsBundle dataBundle = testStorageManagementService.getDataBundleById(badId);
        assertNull(dataBundle);
        verify(bundleDao).findById(badId);
        verify(storageVolumeDao, never()).findById(any(Number.class));
    }

    @Test
    public void useBadIdToGoodBundle() {
        Long goodBundleId = 1L;
        Long volumeId = 2L;
        String testLocation = "volLocation";
        when(bundleDao.findById(goodBundleId))
                .thenReturn(new JacsBundleBuilder()
                        .dataBundleId(goodBundleId)
                        .storageVolumeId(volumeId)
                        .build());
        when(storageVolumeDao.findById(volumeId))
                .thenReturn(new JacsStorageVolumeBuilder()
                        .storageVolumeId(volumeId)
                        .location(testLocation)
                        .build());
        when(storageAgentManager.findRegisteredAgentByLocationOrConnectionInfo(testLocation))
                .thenReturn(Optional.of(new StorageAgentInfo(testLocation,
                        "testURL",
                        "testConnectionInfo",
                        "testPath")));
        JacsBundle dataBundle = testStorageManagementService.getDataBundleById(goodBundleId);
        assertNotNull(dataBundle);
        verify(bundleDao).findById(goodBundleId);
        verify(storageVolumeDao).findById(volumeId);
        verify(storageAgentManager).findRegisteredAgentByLocationOrConnectionInfo(testLocation);
        assertThat(dataBundle.getConnectionInfo(), equalTo("testConnectionInfo"));
    }

    private static class TestAllocateData {
        final Long testBundleId;
        final Long testVolumeId;
        final String testLocation;
        final String testAgentURL;
        final String testAgentConnectionInfo;
        final String testAgentMountPoint;
        final JacsBundle testBundle;
        final JacsStorageVolume testVolume;
        final Map<String, EntityFieldValueHandler<?>> volumeUpdatedFields;

        public TestAllocateData(Long testBundleId,
                                Long testVolumeId,
                                String testLocation,
                                String testAgentURL,
                                String testAgentConnectionInfo,
                                String testAgentMountPoint,
                                JacsBundle testBundle,
                                JacsStorageVolume testVolume,
                                Map<String, EntityFieldValueHandler<?>> volumeUpdatedFields) {
            this.testBundleId = testBundleId;
            this.testVolumeId = testVolumeId;
            this.testLocation = testLocation;
            this.testAgentURL = testAgentURL;
            this.testAgentConnectionInfo = testAgentConnectionInfo;
            this.testAgentMountPoint = testAgentMountPoint;
            this.testBundle = testBundle;
            this.testVolume = testVolume;
            this.volumeUpdatedFields = volumeUpdatedFields;
        }
    }

    @Test
    public void allocateStorageSuccessfully() {
        TestAllocateData testData[] = new TestAllocateData[]{
                new TestAllocateData(10L,
                        20L,
                        "testLocation",
                        "http://agentURL",
                        "agent:100",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L).location("testLocation").build(),
                        ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                                .put("mountHostIP", new SetFieldValueHandler<>("agent:100"))
                                .put("mountPoint", new SetFieldValueHandler<>("/storage"))
                                .put("mountHostURL", new SetFieldValueHandler<>("http://agentURL"))
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        StorageAgentSelector.OVERFLOW_AGENT_INFO,
                        "http://agentURL",
                        "agent:100",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L).location(StorageAgentSelector.OVERFLOW_AGENT_INFO).build(),
                        ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                                .put("mountPoint", new SetFieldValueHandler<>(StorageAgentSelector.OVERFLOW_AGENT_INFO))
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        "testLocation",
                        "http://agentURL",
                        "agent:100",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L).location("testLocation").mountPoint("/storage").build(),
                        ImmutableMap.of()
                )
        };
        for (TestAllocateData td : testData) {
            prepareMockServices(td);
            Optional<JacsBundle> bundleResult = testStorageManagementService.allocateStorage(td.testBundle);
            verifyTestBundle(bundleResult, td, td.volumeUpdatedFields);
        }
    }

    private void prepareMockServices(TestAllocateData testData) {
        Mockito.reset(storageAgentManager, storageVolumeDao, bundleDao);
        when(storageAgentManager.findRandomRegisteredAgent(any(Predicate.class)))
                .thenReturn(Optional.of(new StorageAgentInfo(
                        testData.testLocation,
                        testData.testAgentURL,
                        testData.testAgentConnectionInfo,
                        testData.testAgentMountPoint)));
        when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(testData.testLocation))
                .thenReturn(testData.testVolume);
        doAnswer((invocation) -> {
            JacsBundle bundle = invocation.getArgument(0);
            bundle.setId(testData.testBundleId);
            return null;
        }).when(bundleDao).save(any(JacsBundle.class));
    }

    private void verifyTestBundle(Optional<JacsBundle> bundleResult, TestAllocateData testData, Map<String, EntityFieldValueHandler<?>> volumeUpdatedFields) {
        assertTrue(bundleResult.isPresent());
        Mockito.verify(storageVolumeDao).update(eq(testData.testVolume), refEq(volumeUpdatedFields));
        bundleResult.ifPresent(dataBundle -> {
            assertThat(dataBundle.getStorageVolumeId(), equalTo(testData.testVolumeId));
            assertThat(dataBundle.getConnectionInfo(), equalTo(testData.testAgentConnectionInfo));
            assertThat(dataBundle.getConnectionURL(), equalTo(testData.testAgentURL));
            assertTrue(dataBundle.getStorageVolume().isPresent());
            dataBundle.getStorageVolume().ifPresent(v -> {
                assertThat(v, equalTo(testData.testVolume));
            });
            Mockito.verify(bundleDao).save(dataBundle);
            assertThat(dataBundle.getId(), equalTo(testData.testBundleId));
            assertThat(dataBundle.getPath(), equalTo(testData.testAgentMountPoint + "/" + testData.testBundleId));
            Mockito.verify(bundleDao).update(dataBundle, ImmutableMap.of(
                    "path", new SetFieldValueHandler<>(dataBundle.getPath())
            ));
        });
    }

}
