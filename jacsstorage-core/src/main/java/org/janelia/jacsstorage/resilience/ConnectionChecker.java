package org.janelia.jacsstorage.resilience;

import java.util.function.Consumer;
import java.util.function.Supplier;

public interface ConnectionChecker<S extends ConnectionState> {
    void initialize(Supplier<S> currentStateProvider,
                    ConnectionTester<S> connectionTester,
                    Consumer<S> onSuccess,
                    Consumer<S> onFailure);
    void dispose();
}
