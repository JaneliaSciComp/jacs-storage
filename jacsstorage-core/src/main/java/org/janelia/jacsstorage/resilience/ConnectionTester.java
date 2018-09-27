package org.janelia.jacsstorage.resilience;

public interface ConnectionTester<S extends ConnectionState> {
    S testConnection(S connectionData);
}
