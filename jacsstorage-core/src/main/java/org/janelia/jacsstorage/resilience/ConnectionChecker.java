package org.janelia.jacsstorage.resilience;

import java.util.Optional;
import java.util.function.Consumer;

public interface ConnectionChecker<T> {
    enum ConnectionState {
        OPEN,
        CLOSED,
        HALF_CLOSED
    }
    ConnectionState getState();
    void initialize(T connState,
                    ConnectionTester<T> connectionTester,
                    Consumer<T> onSuccess,
                    Consumer<T> onFailure);
    void dispose();
}
