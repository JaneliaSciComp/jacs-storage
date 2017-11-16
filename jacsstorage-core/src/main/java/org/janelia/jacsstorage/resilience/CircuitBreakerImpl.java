package org.janelia.jacsstorage.resilience;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class CircuitBreakerImpl<T> implements CircuitBreaker<T> {

    private final ScheduledExecutorService scheduler;
    private final int periodInSeconds;
    private final int initialDelayInSeconds;
    private final int tripThreshold;
    private ScheduledFuture<?> updateStateTask;
    private BreakerState state;
    private int numFailures;

    public CircuitBreakerImpl(Optional<BreakerState> initialState,
                              ScheduledExecutorService scheduler,
                              int periodInSeconds,
                              int initialDelayInSeconds,
                              int tripThreshold) {
        initialState.ifPresent(s -> state = s);
        this.scheduler = scheduler;
        this.periodInSeconds = periodInSeconds;
        this.initialDelayInSeconds = initialDelayInSeconds;
        this.tripThreshold = tripThreshold;
    }

    @Override
    public BreakerState getState() {
        return state;
    }

    @Override
    public void initialize(T connState, CircuitTester<T> circuitTester, Optional<Consumer<T>> onSuccess, Optional<Consumer<T>> onFailure) {
        Runnable scheduleTask = () -> {
            if (circuitTester.testConnection(connState)) {
                if (state != BreakerState.CLOSED) {
                    reset();
                    // only invoke the handler if there was a change in the state of the circuit
                    onSuccess.map(action -> {
                        action.accept(connState);
                        return null;
                    });
                }
            } else {
                if (state == null || state != BreakerState.OPEN && ++numFailures >= tripThreshold) {
                    // only invoke the handler if there was a change in the state of the circuit
                    state = BreakerState.OPEN;
                    onFailure.map(action -> {
                        action.accept(connState);
                        return null;
                    });
                } else if (state == BreakerState.CLOSED) {
                    state = BreakerState.HALF_CLOSED;
                }
            }
        };
        if (initialDelayInSeconds == 0) {
            scheduleTask.run();
        }
        dispose();
        updateStateTask = scheduler.scheduleAtFixedRate(scheduleTask, initialDelayInSeconds, periodInSeconds, TimeUnit.SECONDS);
    }

    private void reset() {
        state = BreakerState.CLOSED;
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
