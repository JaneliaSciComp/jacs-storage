package org.janelia.jacsstorage.agent;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.net.InetAddress;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;

@ApplicationScoped
public class AgentState {

    private static final long _1_M = 1024 * 1024;

    @PropertyValue(name = "StorageAgent.IPAddress")
    @Inject
    private String storageIPAddress;
    @PropertyValue(name = "StorageAgent.agentLocation")
    @Inject
    private String agentLocation;
    @PropertyValue(name = "StorageAgent.portNo")
    @Inject
    private Integer agentPortNumber;
    @PropertyValue(name = "StorageAgent.storageRootDir")
    @Inject
    private String storageRootDir;

    public String getAgentLocation() {
        return StringUtils.isBlank(agentLocation) ? getStorageIPAddress() + "/" + getStorageRootDir() : agentLocation;
    }

    private String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getStorageIPAddress() {
        return StringUtils.isBlank(storageIPAddress) ? getCurrentHostIP() : storageIPAddress;
    }

    public String getConnectionInfo() {
        return getStorageIPAddress() + ":" + agentPortNumber;
    }

    public String getStorageRootDir() {
        return storageRootDir;
    }

    public long getAvailableStorageSpaceInMB() {
        try {
            java.nio.file.Path storageRootPath = Paths.get(storageRootDir);
            FileStore storageRootStore = Files.getFileStore(storageRootPath);
            long usableBytes = storageRootStore.getUsableSpace();
            return usableBytes / _1_M;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("agentLocation", agentLocation)
                .append("connectionInfo", getConnectionInfo())
                .append("storageIPAddress", storageIPAddress)
                .append("storageRootDir", storageRootDir)
                .build();
    }
}
