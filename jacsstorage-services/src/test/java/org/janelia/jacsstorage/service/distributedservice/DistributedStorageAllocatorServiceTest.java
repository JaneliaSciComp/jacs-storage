package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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
        final String testAgentURL;
        final String testAgentMountPoint;
        final String testBundlePrefix;
        final JacsBundle testBundle;
        final JacsStorageVolume testVolume;

        public TestAllocateData(Long testBundleId,
                                Long testVolumeId,
                                String testVolumentName,
                                String testHost,
                                String testAgentURL,
                                String testAgentMountPoint,
                                String testBundlePrefix,
                                JacsBundle testBundle,
                                JacsStorageVolume testVolume) {
            this.testBundleId = testBundleId;
            this.testVolumeId = testVolumeId;
            this.testVolumentName = testVolumentName;
            this.testHost = testHost;
            this.testAgentURL = testAgentURL;
            this.testAgentMountPoint = testAgentMountPoint;
            this.testBundlePrefix = testBundlePrefix;
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
                        "http://agentURL",
                        "/storage",
                        null,
                        new JacsBundleBuilder().ownerKey("user:anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .storageHost("testHost")
                                .storageRootDir("/storage")
                                .storageServiceURL("http://agentURL")
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        "testVolumeName",
                        "testHost",
                        "http://agentURL",
                        "/overflowStorage",
                        "bundlePrefix",
                        new JacsBundleBuilder().ownerKey("user:anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .name(JacsStorageVolume.OVERFLOW_VOLUME)
                                .storageRootDir("/overflowStorage")
                                .build()
                ),
                new TestAllocateData(10L,
                        20L,
                        "testVolumeName",
                        "testHost",
                        "http://agentURL",
                        "/storage",
                        "",
                        new JacsBundleBuilder().ownerKey("user:anowner").name("aname").build(),
                        new JacsStorageVolumeBuilder().storageVolumeId(20L)
                                .storageHost("testHost")
                                .storageRootDir("/storage")
                                .storageServiceURL("http://agentURL")
                                .build()
                )
        };
        for (TestAllocateData td : testData) {
            prepareMockServices(td);
            JacsCredentials jacsCredentials = new JacsCredentials();
            Optional<JacsBundle> bundleResult = testStorageAllocatorService.allocateStorage(jacsCredentials, td.testBundlePrefix, td.testBundle);
            verifyTestBundle(bundleResult, td);
        }
    }

    @SuppressWarnings("unchecked")
    private void prepareMockServices(TestAllocateData testData) {
        Mockito.reset(storageAgentManager, storageVolumeDao, bundleDao);
        StorageAgentInfo testAgentInfo = new StorageAgentInfo(
                testData.testHost,
                testData.testAgentURL);
        testAgentInfo.setConnectionStatus("CONNECTED");
        when(storageAgentManager.findRandomRegisteredAgent(any(Predicate.class)))
                .thenReturn(Optional.of(testAgentInfo));
        when(storageAgentManager.findRegisteredAgent(testData.testVolume.getStorageServiceURL()))
                .thenReturn(Optional.of(testAgentInfo));
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
            assertThat(dataBundle.getPath(), equalTo((StringUtils.isBlank(testData.testBundlePrefix) ? "" : testData.testBundlePrefix + "/") + testData.testBundleId.toString()));
            assertThat(dataBundle.getRealStoragePath(), equalTo(Paths.get(testData.testVolume.getStorageRootDir(), StringUtils.defaultIfBlank(testData.testBundlePrefix, ""), testData.testBundleId.toString())));
            Mockito.verify(bundleDao).update(dataBundle, ImmutableMap.of(
                    "path", new SetFieldValueHandler<>(dataBundle.getPath())
            ));
        });
    }

}
