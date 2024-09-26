package org.janelia.jacsstorage.service.impl.distributedservice;

import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.resilience.ConnectionChecker;
import org.janelia.jacsstorage.resilience.ConnectionState;

public class StorageAgentConnection implements ConnectionState {

    static final String CONNECTED_STATUS_VALUE = "CONNECTED";
    static final String DISCONNECTED_STATUS_VALUE = "DISCONNECTED";

    private Status connectStatus;
    private int connectionAttempts;
    private final StorageAgentInfo agentInfo;
    private final ConnectionChecker<StorageAgentConnection> connectionChecker;

    StorageAgentConnection(StorageAgentInfo agentInfo, ConnectionChecker<StorageAgentConnection> connectionChecker) {
        this.agentInfo = agentInfo;
        this.connectionChecker = connectionChecker;
    }

    public StorageAgentInfo getAgentInfo() {
        return agentInfo;
    }

    public ConnectionChecker<StorageAgentConnection> getConnectionChecker() {
        return connectionChecker;
    }

    @Override
    public Status getConnectStatus() {
        return connectStatus;
    }

    @Override
    public void setConnectStatus(Status connectStatus) {
        this.connectStatus = connectStatus;
    }

    @Override
    public int getConnectionAttempts() {
        return connectionAttempts;
    }

    @Override
    public void setConnectionAttempts(int connectionAttempts) {
        this.connectionAttempts = connectionAttempts;
    }

    void updateConnectionStatus(ConnectionState.Status connectStatus) {
        setConnectStatus(connectStatus);
        if (connectStatus == Status.CLOSED) {
            agentInfo.setConnectionStatus(CONNECTED_STATUS_VALUE);
        } else {
            agentInfo.setConnectionStatus(DISCONNECTED_STATUS_VALUE);
        }
    }

    public boolean isConnected() {
        return CONNECTED_STATUS_VALUE.equals(agentInfo.getConnectionStatus());
    }
}
