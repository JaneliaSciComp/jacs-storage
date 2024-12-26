package org.janelia.jacsstorage.service.impl.distributedservice;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionState;
import org.janelia.jacsstorage.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith({MockitoExtension.class})
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

    @BeforeEach
    public void setUp() throws IllegalAccessException {
        MockitoAnnotations.openMocks(this);
        FieldUtils.writeDeclaredField(testStorageAgentManager, "periodInSeconds", periodInSeconds, true);
        FieldUtils.writeDeclaredField(testStorageAgentManager, "initialDelayInSeconds", initialDelayInSeconds, true);
        FieldUtils.writeDeclaredField(testStorageAgentManager, "tripThreshold", tripThreshold, true);
    }

    @Test
    public void registerAgentForTheFirstTime() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

            registerAgent(testAgentHost, testAgentURL, ImmutableSet.of("v1", "v2"), ImmutableSet.of("1"));

            Mockito.verify(scheduler).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));

        }
    }

    private StorageAgentInfo registerAgent(String agentHost, String agentURL, Set<String> servedVolumes, Set<String> unavailableVolumes) {
        StorageAgentInfo agentInfo = new StorageAgentInfo(agentHost, agentURL, servedVolumes, unavailableVolumes);
        return testStorageAgentManager.registerAgent(agentInfo);
    }

    @Test
    public void reRegisterAgent() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";
        Set<String> testServedVolumes = ImmutableSet.of("v1", "v2");
        Set<String> testUnavailableVolumes = ImmutableSet.of("1");

        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {
            StorageAgentInfo firstRegistration = registerAgent(testAgentHost, testAgentURL, testServedVolumes, testUnavailableVolumes);
            StorageAgentInfo secondRegistration = registerAgent(testAgentHost, testAgentURL, testServedVolumes, testUnavailableVolumes);

            assertNotNull(firstRegistration);
            assertNotNull(secondRegistration);

            Mockito.verify(scheduler, Mockito.times(1)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));
            Mockito.verifyNoMoreInteractions(scheduler);
        }
    }

    @Test
    public void reRegisterAgentAfterDeregistration() {
        String testAgentHost = "testHost";
        String testAgentURL = "http://agentURL";
        Set<String> testServedVolumes = ImmutableSet.of("v1", "v2");
        Set<String> testUnavailableVolumes = ImmutableSet.of("1");

        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

            StorageAgentInfo firstRegistration = registerAgent(testAgentHost, testAgentURL, testServedVolumes, testUnavailableVolumes);
            assertNotNull(firstRegistration);
            assertNotNull(testStorageAgentManager.deregisterAgent(testAgentURL, firstRegistration.getAgentToken()));

            StorageAgentInfo secondRegistration = registerAgent(testAgentHost, testAgentURL, testServedVolumes, testUnavailableVolumes);
            assertNotNull(secondRegistration);

            Mockito.verify(scheduler, Mockito.times(2)).scheduleAtFixedRate(any(Runnable.class), eq(initialDelayInSeconds.longValue()), eq(periodInSeconds.longValue()), eq(TimeUnit.SECONDS));

            assertNotEquals(firstRegistration.getAgentToken(), secondRegistration.getAgentToken());
            Mockito.verifyNoMoreInteractions(scheduler);
        }
    }

    @Test
    public void deregisterUnregisteredLocation() {
        assertNull(testStorageAgentManager.deregisterAgent("unregistered", null));
    }

    @Test
    public void checkAgentRandomizedSelection() {
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

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
    }

    @Test
    public void checkAgentRandomizedFilteredSelection() {
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

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
    }

    @Test
    public void findAgentByLocationOrConnectionInfo() {
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

            List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
            registeredAgents.forEach(ai -> {
                assertThat(testStorageAgentManager.findRegisteredAgent(ai.getAgentAccessURL()).orElse(null), equalTo(ai));
            });
            assertNull(testStorageAgentManager.findRegisteredAgent("badLocation").orElse(null));
        }
    }

    @Test
    public void getCurrentRegisteredAgents() {
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.CLOSED);
                                return agentConnection;
                            });
                })) {

            List<StorageAgentInfo> registeredAgents = registerMultipleAgents();
            assertThat(testStorageAgentManager.getCurrentRegisteredAgents(ac -> ac.isConnected()), hasSize(equalTo(registeredAgents.size())));
            registeredAgents.forEach(ai -> {
                assertThat(ai.getConnectionStatus(), equalTo("CONNECTED"));
            });
        }
    }

    @Test
    public void findAgentsWhenThereAreBadConnections() {
        try (MockedConstruction<AgentConnectionTester> mockConnectionTesterConstruction = Mockito.mockConstruction(AgentConnectionTester.class,
                (mockConnectionTester, context) -> {

                    Mockito.when(mockConnectionTester.testConnection(any(StorageAgentConnection.class)))
                            .then((Answer<StorageAgentConnection>) invocation -> {
                                StorageAgentConnection agentConnection = invocation.getArgument(0);
                                agentConnection.updateConnectionStatus(ConnectionState.Status.OPEN);
                                return agentConnection;
                            });
                })) {
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
            StorageAgentInfo agentInfo = testStorageAgentManager.findRandomRegisteredAgent(ac -> ac.isConnected())
                    .orElse(null);
            assertNull(agentInfo);
            registeredAgents.forEach(ai -> {
                StorageAgentInfo agentInfoByLocation = testStorageAgentManager.findRegisteredAgent(ai.getAgentAccessURL()).orElse(null);
                assertNotNull(agentInfoByLocation);
                assertThat(agentInfoByLocation.getConnectionStatus(), equalTo("DISCONNECTED"));
            });
        }
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
        Set<String> testServedVolumes = ImmutableSet.of("v1", "v2");
        Set<String> testUnavailableVolumes = ImmutableSet.of("1");

        StorageAgentInfo agentInfo = new StorageAgentInfo(
                testAgentHost + "_" + index,
                testAgentURL + "_" + index,
                testServedVolumes,
                testUnavailableVolumes);
        return agentInfo;
    }
}
