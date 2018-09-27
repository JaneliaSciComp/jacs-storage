package org.janelia.jacsstorage.service.distributedservice;

import com.google.common.collect.ImmutableList;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.service.NotificationService;
import org.janelia.jacsstorage.service.distributedservice.AgentConnectionTester;
import org.janelia.jacsstorage.service.distributedservice.StorageAgentManagerImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@RunWith(PowerMockRunner.class)
@PrepareForTest(StorageAgentManagerImpl.class)
public class StorageAgentManagerImplTest {

    @Mock
    private ScheduledExecutorService scheduler;
    @Mock
    private NotificationService connectivityNotifier;
    private Integer periodInSeconds = 30;
    private Integer initialDelayInSeconds = 10;
    private Integer tripThreshold = 1;
    @InjectMocks
    private StorageAgentManagerImpl testStorageAgentManager;
    private AgentConnectionTester mockConnectionTester;

    @Before
    public void setUp() throws IllegalAccessException {
        MockitoAnnotations.initMocks(this);
        PowerMockito.field(StorageAgentManagerImpl.class, "periodInSeconds").set(testStorageAgentManager, periodInSeconds);
        PowerMockito.field(StorageAgentManagerImpl.class, "initialDelayInSeconds").set(testStorageAgentManager, initialDelayInSeconds);
        PowerMockito.field(StorageAgentManagerImpl.class, "tripThreshold").set(testStorageAgentManager, tripThreshold);
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
        mockConnectionTester = Mockito.mock(AgentConnectionTester.class);
        try {
            PowerMockito.whenNew(AgentConnectionTester.class).withNoArguments().thenReturn(mockConnectionTester);
        } catch (Exception e) {
            fail("Failed to mock a connection tester " + e);
        }
    }

    private void prepareConnectionTester(boolean result) {
        Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                .then((Answer<StorageAgentConnection>) invocation -> {
                    StorageAgentConnection agentConnection = invocation.getArgument(0);
                    agentConnection.updateConnectionStatus(result
                            ? ConnectionState.Status.CLOSED
                            : ConnectionState.Status.OPEN);
                    return agentConnection;
                });
    }

    @Test
    public void registerAgentForTheFirstTime() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";

        prepareConnectionTester(true);
        registerAgent(testAgentHost, testAgentURL);

        Mockito.verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));
    }

    private StorageAgentInfo registerAgent(String agentHost, String agentURL) {
        StorageAgentInfo agentInfo = new StorageAgentInfo(agentHost, agentURL);
        return testStorageAgentManager.registerAgent(agentInfo);
    }

    @Test
    public void reRegisterAgent() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";

        prepareConnectionTester(true);
        StorageAgentInfo firstRegistration = registerAgent(testAgentHost, testAgentURL);
        StorageAgentInfo secondRegistration = registerAgent(testAgentHost, testAgentURL);

        assertNotNull(firstRegistration);
        assertNotNull(secondRegistration);

        Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));
        Mockito.verifyNoMoreInteractions(scheduler);
    }

    @Test
    public void reRegisterAgentAfterDeregistration() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";

        prepareConnectionTester(true);
        StorageAgentInfo firstRegistration = registerAgent(testAgentHost, testAgentURL);
        assertNotNull(firstRegistration);
        assertNotNull(testStorageAgentManager.deregisterAgent(testAgentURL, firstRegistration.getAgentToken()));

        StorageAgentInfo secondRegistration = registerAgent(testAgentHost, testAgentURL);
        assertNotNull(secondRegistration);

        Mockito.verify(scheduler, Mockito.times(2)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));

        assertNotEquals(firstRegistration.getAgentToken(), secondRegistration.getAgentToken());
        Mockito.verifyNoMoreInteractions(scheduler);
    }

    @Test
    public void deregisterUnregisteredLocation() {
        assertNull(testStorageAgentManager.deregisterAgent("unregistered", null));
    }

    @Test
    public void checkAgentRandomizedSelection() {
        prepareConnectionTester(true);
        Map<StorageAgentInfo, Integer> registeredAgents = registerMultipleAgents().stream().collect(Collectors.toMap(ai -> ai, ai -> 0));
        int nInvocations = registeredAgents.size() * 3;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ac -> true)
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
        prepareConnectionTester(true);
        Map<StorageAgentInfo, Integer> registeredAgents = registerMultipleAgents().stream().collect(Collectors.toMap(ai -> ai, ai -> 0));
        int nInvocations = registeredAgents.size() * 3;
        for (int i = 0; i < nInvocations; i++) {
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ac -> ac.isConnected())
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
    public void findAgentByLocationOrConnectionInfo() {
        prepareConnectionTester(true);
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        registeredAgents.forEach(ai -> {
            assertThat(testStorageAgentManager.findRegisteredAgent(ai.getAgentHttpURL()).orElse(null), equalTo(ai));
        });
        assertNull(testStorageAgentManager.findRegisteredAgent("badLocation").orElse(null));
    }

    @Test
    public void getCurrentRegisteredAgents() {
        prepareConnectionTester(true);
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        assertThat(testStorageAgentManager.getCurrentRegisteredAgents(ac -> ac.isConnected()), hasSize(equalTo(registeredAgents.size())));
        registeredAgents.forEach(ai -> {
            assertThat(ai.getConnectionStatus(), equalTo("CONNECTED"));
        });
    }

    @Test
    public void findAgentsWhenThereAreBadConnections() {
        prepareConnectionTester(false);
        List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
        StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ac -> ac.isConnected())
                .orElse(null);
        assertNull(agentInfo);
        registeredAgents.forEach(ai -> {
            StorageAgentInfo agentInfoByLocation = testStorageAgentManager.findRegisteredAgent(ai.getAgentHttpURL()).orElse(null);
            assertNotNull(agentInfoByLocation);
            assertThat(agentInfoByLocation.getConnectionStatus(), equalTo("DISCONNECTED"));
        });
    }

    private List<StorageAgentInfo> registerMultipleAgents() {
        int agentIndex = 0;
        List<StorageAgentInfo> testAgents = ImmutableList.<StorageAgentInfo>builder()
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .add(prepareTestAgentInfo(++agentIndex))
                .build();
        testAgents.forEach(agentInfo -> testStorageAgentManager.registerAgent(agentInfo));
        return testAgents;
    }

    private StorageAgentInfo prepareTestAgentInfo(int index) {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";
        StorageAgentInfo agentInfo = new StorageAgentInfo(testAgentHost + "_" + index,
                testAgentURL + "_" + index);
        return agentInfo;
    }
}
