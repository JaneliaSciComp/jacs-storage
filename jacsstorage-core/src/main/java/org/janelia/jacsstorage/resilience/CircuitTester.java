package org.janelia.jacsstorage.resilience;

public interface CircuitTester<T> {
    boolean testConnection(T connectionState);
}
