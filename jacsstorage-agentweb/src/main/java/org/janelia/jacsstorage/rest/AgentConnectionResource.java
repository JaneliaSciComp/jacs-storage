package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;

import javax.annotation.security.PermitAll;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("connection")
public class AgentConnectionResource {

    @Inject
    private AgentState agentState;
    @Context
    private UriInfo resourceURI;

    @PermitAll
    @Path("status")
    @GET
    public Response getStatus() {
        StorageAgentInfo localAgentInfo = agentState.getLocalAgentInfo();
        return Response
                .ok(localAgentInfo)
                .build();
    }

    @PermitAll
    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    public Response connect(String connectURL) {
        agentState.connectTo(connectURL);
        StorageAgentInfo localAgentInfo = agentState.getLocalAgentInfo();
        return Response
                .created(resourceURI.getRequestUriBuilder().path("status").path(localAgentInfo.getLocation()).build())
                .entity(localAgentInfo)
                .build();
    }

}
