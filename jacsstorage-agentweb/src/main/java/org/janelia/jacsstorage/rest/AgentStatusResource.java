package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.agent.AgentState;
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

@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("status")
public class AgentStatusResource {

    @Inject
    private AgentState agentState;
    @Context
    private UriInfo resourceURI;

    @GET
    public Response getStatus() {
        StorageAgentInfo localAgentInfo = new StorageAgentInfo(agentState.getAgentLocation(), agentState.getConnectionInfo(), agentState.getStorageRootDir());
        localAgentInfo.setStorageSpaceAvailableInMB(agentState.getAvailableStorageSpaceInMB());
        return Response
                .ok(localAgentInfo)
                .build();
    }

}
