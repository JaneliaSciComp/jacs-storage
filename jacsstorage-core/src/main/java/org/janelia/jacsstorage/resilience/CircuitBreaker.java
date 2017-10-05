package org.janelia.jacsstorage.resilience;

import java.util.Optional;
import java.util.function.Consumer;

public interface CircuitBreaker<T> {
    enum BreakerState {
        OPEN,
        CLOSED,
        HALF_CLOSED
    }
    BreakerState getState();
    void initialize(T connState, CircuitTester<T> circuitTester, Optional<Consumer<T>> onSuccess, Optional<Consumer<T> >onFailure);
    void dispose();
}
