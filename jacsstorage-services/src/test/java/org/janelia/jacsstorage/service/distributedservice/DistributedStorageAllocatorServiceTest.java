package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.EntityFieldValueHandler;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
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
        final String testVolumentName;
        final String testHost;
        final int testPort;
        final String testAgentURL;
        final String testAgentMountPoint;
        final JacsBundle testBundle;
        final JacsStorageVolume testVolume;

        public TestAllocateData(Long testBundleId,
                                Long testVolumeId,
                                String testVolumentName,
                                String testHost,
                                int testPort,
                                String testAgentURL,
                                String testAgentMountPoint,
                                JacsBundle testBundle,
                                JacsStorageVolume testVolume) {
            this.testBundleId = testBundleId;
            this.testVolumeId = testVolumeId;
            this.testVolumentName = testVolumentName;
            this.testHost = testHost;
            this.testPort = testPort;
            this.testAgentURL = testAgentURL;
            this.testAgentMountPoint = testAgentMountPoint;
            this.testBundle = testBundle;
            this.testVolume = testVolume;
        }
    }

    @Test
    public void allocateStorageSuccessfully() {
        TestAllocateData testData[] = new TestAllocateData[]{
                new TestAllocateData(10L,
                        20L,
                        "testVolumeName",
                        "testHost",
                        100,
                        "http://agentURL",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .storageHost("testHost")
                                .volumePath("/storage")
                                .storageServiceURL("http://agentURL")
                                .tcpPortNo(100)
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        "testVolumeName",
                        "testHost",
                        100,
                        "http://agentURL",
                        "/overflowStorage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .name(JacsStorageVolume.OVERFLOW_VOLUME)
                                .volumePath("/overflowStorage")
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        "testVolumeName",
                        "testHost",
                        100,
                        "http://agentURL",
                        "/storage",
                        new JacsBundleBuilder().owner("anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .storageHost("testHost")
                                .volumePath("/storage")
                                .storageServiceURL("http://agentURL")
                                .build()
                )
        };
        for (TestAllocateData td : testData) {
            prepareMockServices(td);
            JacsCredentials jacsCredentials = new JacsCredentials();
            Optional<JacsBundle> bundleResult = testStorageAllocatorService.allocateStorage(jacsCredentials, td.testBundle);
            verifyTestBundle(bundleResult, td);
        }
    }

    @SuppressWarnings("unchecked")
    private void prepareMockServices(TestAllocateData testData) {
        Mockito.reset(storageAgentManager, storageVolumeDao, bundleDao);
        when(storageAgentManager.findRandomRegisteredAgent(any(Predicate.class)))
                .thenReturn(Optional.of(new StorageAgentInfo(
                        testData.testHost,
                        testData.testAgentURL,
                        testData.testPort)));
        when(storageVolumeDao.countMatchingVolumes(any(StorageQuery.class)))
                .then(invocation -> {
                    if (JacsStorageVolume.OVERFLOW_VOLUME.equals(testData.testVolume.getName())) {
                        return 0L;
                    } else {
                        return 1L;
                    }
                });
        when(storageVolumeDao.findMatchingVolumes(any(StorageQuery.class), any(PageRequest.class)))
                .then(invocation -> new PageResult<>(invocation.getArgument(1), ImmutableList.of(testData.testVolume)));
        doAnswer((invocation) -> {
            JacsBundle bundle = invocation.getArgument(0);
            bundle.setId(testData.testBundleId);
            return null;
        }).when(bundleDao).save(any(JacsBundle.class));
    }

    private void verifyTestBundle(Optional<JacsBundle> bundleResult, TestAllocateData testData) {
        assertTrue(bundleResult.isPresent());
        bundleResult.ifPresent(dataBundle -> {
            assertThat(dataBundle.getStorageVolumeId(), equalTo(testData.testVolumeId));
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
