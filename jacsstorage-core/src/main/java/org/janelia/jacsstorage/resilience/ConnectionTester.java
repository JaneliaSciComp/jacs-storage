package org.janelia.jacsstorage.resilience;

public interface ConnectionTester<T> {
    boolean testConnection(T connectionData);
}
