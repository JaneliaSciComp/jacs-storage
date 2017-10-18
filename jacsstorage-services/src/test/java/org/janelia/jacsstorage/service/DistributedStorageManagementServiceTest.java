package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.dao.JacsBundleDao;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

}
