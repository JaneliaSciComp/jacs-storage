package org.janelia.jacsstorage.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class PeriodicConnectionChecker<T> implements ConnectionChecker<T> {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicConnectionChecker.class);
    private final ScheduledExecutorService scheduler;
    private final int periodInSeconds;
    private final int initialDelayInSeconds;
    private final int tripThreshold;
    private ScheduledFuture<?> updateStateTask;
    private ConnectionState state;
    private int numFailures;

    public PeriodicConnectionChecker(ConnectionState initialState,
                                     ScheduledExecutorService scheduler,
                                     int periodInSeconds,
                                     int initialDelayInSeconds,
                                     int tripThreshold) {
        if (initialState != null) {
            this.state = initialState;
        }
        this.scheduler = scheduler;
        this.periodInSeconds = periodInSeconds;
        this.initialDelayInSeconds = initialDelayInSeconds;
        this.tripThreshold = tripThreshold;
    }

    @Override
    public ConnectionState getState() {
        return state;
    }

    @Override
    public void initialize(T connState,
                           ConnectionTester<T> connectionTester,
                           Consumer<T> onSuccess,
                           Consumer<T> onFailure) {
        Runnable scheduleTask = () -> {
            try {
                if (connectionTester.testConnection(connState)) {
                    if (state != ConnectionState.CLOSED) {
                        reset();
                        // only invoke the handler if there was a change in the state of the circuit
                        if (onSuccess != null) {
                            onSuccess.accept(connState);
                        }
                    }
                } else {
                    if (state == null || state != ConnectionState.OPEN && ++numFailures >= tripThreshold) {
                        // only invoke the handler if there was a change in the state of the circuit
                        state = ConnectionState.OPEN;
                        if (onFailure != null) {
                            onFailure.accept(connState);
                        }
                    } else if (state == ConnectionState.CLOSED) {
                        state = ConnectionState.HALF_CLOSED;
                    }
                }
            } catch (Exception e) {
                LOG.error("Error testing connection", e);
                if (onFailure != null && state != ConnectionState.OPEN) {
                    onFailure.accept(connState);
                }
                state = ConnectionState.OPEN;
            }
        };
        if (initialDelayInSeconds == 0) {
            scheduleTask.run();
        }
        dispose();
        updateStateTask = scheduler.scheduleAtFixedRate(scheduleTask, initialDelayInSeconds, periodInSeconds, TimeUnit.SECONDS);
    }

    private void reset() {
        state = ConnectionState.CLOSED;
        numFailures = 0;
    }

    @Override
    public void dispose() {
        if (updateStateTask != null) {
            updateStateTask.cancel(false);
            updateStateTask = null;
        }
    }
}
