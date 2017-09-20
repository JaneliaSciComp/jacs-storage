package org.janelia.jacsstorage.agent;

import org.apache.commons.lang3.StringUtils;
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
        return StringUtils.isBlank(agentLocation) ? getCurrentHostIP() + "/" + getStorageRootDir() : agentLocation;
    }

    private String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    public String getConnectionInfo() {
        return getCurrentHostIP() + ":" + agentPortNumber;
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
}
