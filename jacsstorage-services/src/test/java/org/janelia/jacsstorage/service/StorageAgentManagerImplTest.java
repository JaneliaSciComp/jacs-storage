package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class StorageAgentManagerImplTest {

    @Mock
    private JacsStorageVolumeDao storageVolumeDao;
    @Mock
    private ScheduledExecutorService scheduler;
    private Integer periodInSeconds = 30;
    private Integer initialDelayInSeconds = 10;
    private Integer tripThreshold = 1;
    @InjectMocks
    private StorageAgentManagerImpl testStorageAgentManager;

    @Before
    public void setUp() throws IllegalAccessException {
        MockitoAnnotations.initMocks(this);
        PowerMockito.field(StorageAgentManagerImpl.class, "periodInSeconds").set(testStorageAgentManager, periodInSeconds);
        PowerMockito.field(StorageAgentManagerImpl.class, "initialDelayInSeconds").set(testStorageAgentManager, initialDelayInSeconds);
        PowerMockito.field(StorageAgentManagerImpl.class, "tripThreshold").set(testStorageAgentManager, tripThreshold);
    }

    @Test
    public void registerAgentForTheFirstTime() {
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent:100";
        String testAgentMountPoint = "/storage";
        JacsStorageVolume testVolume = new JacsStorageVolume();
        Mockito.when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(testLocation)).thenReturn(testVolume);

        registerAgent(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint);

        Mockito.verify(storageVolumeDao).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountPoint", new SetFieldValueHandler<>(testAgentMountPoint),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        Mockito.verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));
    }

    private StorageAgentInfo registerAgent(String agentLocation, String agentURL, String agentConnectionInfo, String agentMountVolume) {
        StorageAgentInfo agentInfo = new StorageAgentInfo(agentLocation, agentURL, agentConnectionInfo, agentMountVolume);
        return testStorageAgentManager.registerAgent(agentInfo);
    }

    @Test
    public void reRegisterAgent() {
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent:100";
        String testAgentMountPoint = "/storage";
        JacsStorageVolume testVolume = new JacsStorageVolume();
        Mockito.when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(testLocation)).thenReturn(testVolume);
        StorageAgentInfo firstRegistration = registerAgent(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint);
        StorageAgentInfo secondRegistration = registerAgent(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint);

        assertNotNull(firstRegistration);
        assertNotNull(secondRegistration);

        Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));

        Mockito.verify(storageVolumeDao, Mockito.times(1)).getStorageByLocationAndCreateIfNotFound(testLocation);
        Mockito.verify(storageVolumeDao, Mockito.times(1)).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountPoint", new SetFieldValueHandler<>(testAgentMountPoint),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        Mockito.verifyNoMoreInteractions(scheduler);
    }

    @Test
    public void reRegisterAgentAfterDeregistration() {
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent:100";
        String testAgentMountPoint = "/storage";
        JacsStorageVolume testVolume = new JacsStorageVolume();
        Mockito.when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(testLocation)).thenReturn(testVolume);

        StorageAgentInfo firstRegistration = registerAgent(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint);
        assertNotNull(firstRegistration);
        assertNotNull(testStorageAgentManager.deregisterAgent(testLocation));

        StorageAgentInfo secondRegistration = registerAgent(testLocation, testAgentURL, testAgentConnectionInfo, testAgentMountPoint);
        assertNotNull(secondRegistration);

        Mockito.verify(scheduler, Mockito.times(2)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));

        Mockito.verify(storageVolumeDao, Mockito.times(2)).getStorageByLocationAndCreateIfNotFound(testLocation);
        Mockito.verify(storageVolumeDao, Mockito.times(2)).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountPoint", new SetFieldValueHandler<>(testAgentMountPoint),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        Mockito.verifyNoMoreInteractions(scheduler);
    }

    @Test
    public void deregisterUnregisteredLocation() {
        assertNull(testStorageAgentManager.deregisterAgent("unregistered"));
    }
}
