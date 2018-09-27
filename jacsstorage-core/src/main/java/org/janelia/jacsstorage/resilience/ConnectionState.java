package org.janelia.jacsstorage.resilience;

public interface ConnectionState {
    enum Status {
        OPEN,
        CLOSED,
        HALF_CLOSED
    }

    Status getConnectStatus();
    void setConnectStatus(Status connectStatus);
    int getConnectionAttempts();
    void setConnectionAttempts(int  connectionAttempts);

    default boolean isConnected() {
        return getConnectStatus() == Status.CLOSED;
    }

    default boolean isNotConnected() {
        return !isConnected();
    }
}
