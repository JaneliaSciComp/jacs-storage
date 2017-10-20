package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Optional;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
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

    @Test
    public void allocateStorageOnNewVolume() {
        Long testBundleId = 10L;
        Long testVolumeId = 20L;
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent:100";
        String testAgentMountPoint = "/storage";
        when(storageAgentManager.findRandomRegisteredAgent(any(Predicate.class)))
                .thenReturn(Optional.of(new StorageAgentInfo(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint)));
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder().storageVolumeId(testVolumeId).location(testLocation).build();
        when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(testLocation))
                .thenReturn(testVolume);
        doAnswer((invocation) -> {
            JacsBundle bundle = invocation.getArgument(0);
            bundle.setId(testBundleId);
            return null;
        }).when(bundleDao).save(any(JacsBundle.class));

        JacsBundle testBundle = new JacsBundleBuilder().owner("anowner").name("aname").build();
        Optional<JacsBundle> bundleResult = testStorageManagementService.allocateStorage(testBundle);
        assertTrue(bundleResult.isPresent());
        Mockito.verify(storageVolumeDao).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountPoint", new SetFieldValueHandler<>(testAgentMountPoint),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        bundleResult.ifPresent(dataBundle -> {
            assertThat(dataBundle.getStorageVolumeId(), equalTo(testVolumeId));
            assertThat(dataBundle.getConnectionInfo(), equalTo(testAgentConnectionInfo));
            assertThat(dataBundle.getConnectionURL(), equalTo(testAgentURL));
            assertTrue(dataBundle.getStorageVolume().isPresent());
            dataBundle.getStorageVolume().ifPresent(v -> {
                assertThat(v, equalTo(testVolume));
            });
            Mockito.verify(bundleDao).save(dataBundle);
            assertThat(dataBundle.getId(), equalTo(testBundleId));
            assertThat(dataBundle.getPath(), equalTo(testAgentMountPoint + "/" + testBundleId));
            Mockito.verify(bundleDao).update(dataBundle, ImmutableMap.of(
                    "path", new SetFieldValueHandler<>(dataBundle.getPath())
            ));
        });
    }
}
