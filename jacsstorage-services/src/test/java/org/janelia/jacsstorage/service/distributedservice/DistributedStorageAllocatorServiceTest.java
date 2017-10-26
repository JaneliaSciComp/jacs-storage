package org.janelia.jacsstorage.service.distributedservice;

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
import org.janelia.jacsstorage.security.JacsCredentials;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DistributedStorageAllocatorServiceTest {

    private StorageAgentManager storageAgentManager;
    private JacsStorageVolumeDao storageVolumeDao;
    private JacsBundleDao bundleDao;
    private DistributedStorageAllocatorService testStorageAllocatorService;

    @Before
    public void setUp() {
        storageAgentManager = mock(StorageAgentManager.class);
        storageVolumeDao = mock(JacsStorageVolumeDao.class);
        bundleDao = mock(JacsBundleDao.class);
        testStorageAllocatorService = new DistributedStorageAllocatorService(storageVolumeDao, bundleDao, storageAgentManager);
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
                        StorageAgentInfo.OVERFLOW_AGENT,
                        "http://agentURL",
                        "agent:100",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L).location(StorageAgentInfo.OVERFLOW_AGENT).build(),
                        ImmutableMap.<String, EntityFieldValueHandler<?>>builder()
                                .put("mountPoint", new SetFieldValueHandler<>(StorageAgentInfo.OVERFLOW_AGENT))
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
            JacsCredentials jacsCredentials = new JacsCredentials();
            Optional<JacsBundle> bundleResult = testStorageAllocatorService.allocateStorage(jacsCredentials, td.testBundle);
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
