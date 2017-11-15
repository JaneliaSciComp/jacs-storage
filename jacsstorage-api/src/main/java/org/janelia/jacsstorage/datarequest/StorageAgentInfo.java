package org.janelia.jacsstorage.datarequest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class StorageAgentInfo {

    private final String storageHost;
    private final String agentHttpURL;
    private final int tcpPortNo;
    private String connectionStatus;
    private String agentToken;

    @JsonCreator
    public StorageAgentInfo(@JsonProperty("storageHost") String storageHost,
                            @JsonProperty("agentHttpURL") String agentHttpURL,
                            @JsonProperty("tcpPortNo") int tcpPortNo) {
        this.storageHost = storageHost;
        this.agentHttpURL = agentHttpURL;
        this.tcpPortNo = tcpPortNo;
    }

    public String getStorageHost() {
        return storageHost;
    }

    public String getAgentHttpURL() {
        return agentHttpURL;
    }

    public int getTcpPortNo() {
        return tcpPortNo;
    }

    public String getConnectionStatus() {
        return connectionStatus;
    }

    public void setConnectionStatus(String connectionStatus) {
        this.connectionStatus = connectionStatus;
    }

    public String getAgentToken() {
        return agentToken;
    }

    public void setAgentToken(String agentToken) {
        this.agentToken = agentToken;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("storageHost", storageHost)
                .append("agentHttpURL", agentHttpURL)
                .append("tcpPortNo", tcpPortNo)
                .append("agentToken", agentToken)
                .toString();
    }

}
