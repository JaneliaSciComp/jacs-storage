package org.janelia.jacsstorage.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class PeriodicConnectionChecker<S extends ConnectionState> implements ConnectionChecker<S> {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicConnectionChecker.class);
    private final ScheduledExecutorService scheduler;
    private final int periodInSeconds;
    private final int initialDelayInSeconds;
    private final int tripThreshold;
    private ScheduledFuture<?> updateStateTask;

    public PeriodicConnectionChecker(ScheduledExecutorService scheduler,
                                     int periodInSeconds,
                                     int initialDelayInSeconds,
                                     int tripThreshold) {
        this.scheduler = scheduler;
        this.periodInSeconds = periodInSeconds;
        this.initialDelayInSeconds = initialDelayInSeconds;
        this.tripThreshold = tripThreshold;
    }

    @Override
    public void initialize(Supplier<S> currentStateProvider,
                           ConnectionTester<S> connectionTester,
                           Consumer<S> onSuccess,
                           Consumer<S> onFailure) {
        Runnable scheduleTask = () -> {
            S prevConnState = currentStateProvider.get();
            try {
                S newConnState = connectionTester.testConnection(prevConnState);
                if (newConnState.isConnected()) {
                    if (prevConnState.isNotConnected()) {
                        // only invoke the handler if there was a change in the state of the circuit
                        onSuccess.accept(newConnState);
                    }
                } else {
                    handleConnectionFailure(prevConnState.getConnectStatus(), newConnState, onFailure);
                }
            } catch (Exception e) {
                if (LOG.isDebugEnabled()) {
                    LOG.error("Error testing connection", e);
                } else {
                    LOG.error("Error testing connection {}", e.toString());
                }
                handleConnectionFailure(prevConnState.getConnectStatus(), prevConnState, onFailure);
            }
        };
        if (initialDelayInSeconds == 0) {
            scheduleTask.run();
        }
        dispose();
        updateStateTask = scheduler.scheduleAtFixedRate(scheduleTask, initialDelayInSeconds, periodInSeconds, TimeUnit.SECONDS);
    }

    private void handleConnectionFailure(ConnectionState.Status prevConnStatus, S newConnState, Consumer<S> onFailure) {
        int connectionAttempts = newConnState.getConnectionAttempts();
        if (prevConnStatus != ConnectionState.Status.OPEN && connectionAttempts >= tripThreshold) {
            newConnState.setConnectStatus(ConnectionState.Status.OPEN);
            onFailure.accept(newConnState);
        } else if (prevConnStatus == ConnectionState.Status.CLOSED) {
            newConnState.setConnectStatus(ConnectionState.Status.HALF_CLOSED);
        }
    }

    @Override
    public void dispose() {
        if (updateStateTask != null) {
            updateStateTask.cancel(false);
            updateStateTask = null;
        }
    }
}
