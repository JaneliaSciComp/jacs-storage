package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.PropertyValue;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.net.InetAddress;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;

@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("status")
public class AgentStatusResource {

    private static final long _1_M = 1024 * 1024;

    @PropertyValue(name = "StorageAgent.agentLocation")
    @Inject
    private String agentLocation;
    @PropertyValue(name = "App.PortNumber")
    @Inject
    private Integer agentPortNumber;
    private String connectionInfo;
    @PropertyValue(name = "StorageAgent.storageRootDir")
    @Inject
    private String storageRootDir;
    @Context
    private UriInfo resourceURI;

    @GET
    public Response getStatus() {
        if (StringUtils.isBlank(agentLocation)) {
            agentLocation = getCurrentHostIP();
        }
        if (StringUtils.isBlank(connectionInfo)) {
            connectionInfo = agentLocation + ":" + agentPortNumber;
        }
        StorageAgentInfo localAgentInfo = new StorageAgentInfo(agentLocation, connectionInfo, storageRootDir);
        localAgentInfo.setStorageSpaceAvailableInMB(getAvailableStorageSpace());
        return Response
                .ok(localAgentInfo)
                .build();
    }

    private String getCurrentHostIP() {
        try {
            InetAddress ip = InetAddress.getLocalHost();
            return ip.getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private long getAvailableStorageSpace() {
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
