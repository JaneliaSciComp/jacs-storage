package org.janelia.jacsstorage.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.dao.JacsStorageVolumeDao;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolumeBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.model.support.SetFieldValueHandler;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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

    @Test
    public void registerAgentWithDifferentVolume() {
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent:100";
        String testAgentMountPoint = "/storage";
        JacsStorageVolume testVolume = new JacsStorageVolumeBuilder().mountPoint("/oldstorage").build();
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
        Mockito.verify(storageVolumeDao, Mockito.times(1)).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountPoint", new SetFieldValueHandler<>(testAgentMountPoint),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        Mockito.verify(storageVolumeDao, Mockito.times(1)).update(testVolume, ImmutableMap.of(
                "mountHostIP", new SetFieldValueHandler<>(testAgentConnectionInfo),
                "mountHostURL", new SetFieldValueHandler<>(testAgentURL)
        ));
        Mockito.verifyNoMoreInteractions(scheduler);
    }

    @Test
    public void deregisterUnregisteredLocation() {
        assertNull(testStorageAgentManager.deregisterAgent("unregistered"));
    }

    @Test
    public void checkAgentRandomizedSelection() {
        Map<StorageAgentInfo, Integer> registeredAgents = registerMultipleAgents().stream().collect(Collectors.toMap(ai -> ai, ai -> 0));
        int nInvocations = registeredAgents.size() * 3;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(null)
                .orElse(null);
            assertNotNull(agentInfo);
            registeredAgents.put(agentInfo, registeredAgents.get(agentInfo) + 1);
        }
        // check that the manager didn't always returned the same agent
        registeredAgents.forEach((ai, count) -> {
            assertThat(count, greaterThanOrEqualTo(0));
            assertThat(count, lessThan(nInvocations));
        });
    }

    @Test
    public void checkAgentRandomizedFilteredSelection() {
        Map<StorageAgentInfo, Integer> registeredAgents = registerMultipleAgents().stream().collect(Collectors.toMap(ai -> ai, ai -> 0));
        int nInvocations = registeredAgents.size() * 3;
        long requestedSpace = 8;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ai -> ai.getStorageSpaceAvailableInMB() > requestedSpace)
                    .orElse(null);
            assertNotNull(agentInfo);
            registeredAgents.put(agentInfo, registeredAgents.get(agentInfo) + 1);
        }
        // check that the manager didn't always returned the same agent
        registeredAgents.forEach((ai, count) -> {
            if (ai.getStorageSpaceAvailableInMB() <= requestedSpace) {
                assertThat(count, equalTo(0));
            } else {
                assertThat(count, greaterThanOrEqualTo(0));
                assertThat(count, lessThan(nInvocations));
            }
        });
    }

    @Test
    public void findAgentWhenASingleOptionIsAvailable() {
        Map<StorageAgentInfo, Integer> registeredAgents = registerMultipleAgents().stream().collect(Collectors.toMap(ai -> ai, ai -> 0));
        int nInvocations = registeredAgents.size() * 3;
        long requestedSpace = 15;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ai -> ai.getStorageSpaceAvailableInMB() > requestedSpace)
                    .orElse(null);
            assertNotNull(agentInfo);
            registeredAgents.put(agentInfo, registeredAgents.get(agentInfo) + 1);
        }
        registeredAgents.forEach((ai, count) -> {
            if (ai.getStorageSpaceAvailableInMB() <= requestedSpace) {
                assertThat(count, equalTo(0));
            } else {
                assertThat(count, equalTo(nInvocations));
            }
        });
    }

    @Test
    public void findAgentWhenNoOptionIsAvailable() {
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        int nInvocations = registeredAgents.size() * 3;
        long requestedSpace = 100;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ai -> ai.getStorageSpaceAvailableInMB() > requestedSpace)
                    .orElse(null);
            assertNull(agentInfo);
        }
    }

    @Test
    public void findAgentByLocationOrConnectionInfo() {
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        registeredAgents.forEach(ai -> {
            assertThat(testStorageAgentManager.findRegisteredAgentByLocationOrConnectionInfo(ai.getLocation()).orElse(null), equalTo(ai));
            assertThat(testStorageAgentManager.findRegisteredAgentByLocationOrConnectionInfo(ai.getConnectionInfo()).orElse(null), equalTo(ai));
        });
        assertNull(testStorageAgentManager.findRegisteredAgentByLocationOrConnectionInfo("badLocation").orElse(null));
    }

    @Test
    public void getCurrentRegisteredAgents() {
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        assertThat(testStorageAgentManager.getCurrentRegisteredAgents(), hasSize(equalTo(registeredAgents.size())));
        registeredAgents.forEach(ai -> {
            assertThat(ai.getConnectionStatus(), equalTo("CONNECTED"));
        });
    }

    @Test
    public void findAgentsWhenThereAreBadConnections() {
        Mockito.when(scheduler.scheduleAtFixedRate(
                any(Runnable.class),
                eq(initialDelayInSeconds.longValue()),
                eq(periodInSeconds.longValue()),
                eq(TimeUnit.SECONDS)))
                .then((Answer<ScheduledFuture<?>>) invocation -> {
                    Runnable r = invocation.getArgument(0);
                    r.run();
                    return null;
                });
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(null)
                .orElse(null);
        assertNull(agentInfo);
        registeredAgents.forEach(ai -> {
            StorageAgentInfo agentInfoByLocation = testStorageAgentManager.findRegisteredAgentByLocationOrConnectionInfo(ai.getLocation()).orElse(null);
            assertNotNull(agentInfoByLocation);
            assertThat(agentInfoByLocation.getConnectionStatus(), equalTo("DISCONNECTED"));
        });
    }

    private List<StorageAgentInfo> registerMultipleAgents() {
        int agentIndex = 0;
        List<StorageAgentInfo> testAgents = ImmutableList.<StorageAgentInfo>builder()
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 5))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 10))
                .add(prepareTestAgentInfo(++agentIndex, 20))
                .build();
        testAgents.forEach(agentInfo -> testStorageAgentManager.registerAgent(agentInfo));
        return testAgents;
    }

    private StorageAgentInfo prepareTestAgentInfo(int index, long availableSpace) {
        String testLocation = "testLocation";
        String testAgentURL = "http://agentURL";
        String testAgentConnectionInfo = "agent";
        String testAgentMountPoint = "/storage";
        StorageAgentInfo agentInfo = new StorageAgentInfo(testLocation + "_" + index,
                testAgentURL + "_" + index,
                testAgentConnectionInfo + "_" + index + ":100",
                testAgentMountPoint);
        agentInfo.setStorageSpaceAvailableInMB(availableSpace);
        JacsStorageVolume testVolume = new JacsStorageVolume();
        Mockito.when(storageVolumeDao.getStorageByLocationAndCreateIfNotFound(agentInfo.getLocation())).thenReturn(testVolume);
        return agentInfo;
    }
}
