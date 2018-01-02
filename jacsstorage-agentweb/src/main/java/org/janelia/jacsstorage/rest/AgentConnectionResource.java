package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;

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
@Api(value = "Agent connectivity API")
public class AgentConnectionResource {

    @Inject
    private AgentState agentState;
    @Context
    private UriInfo resourceURI;

    @Path("status")
    @GET
    public Response getStatus() {
        StorageAgentInfo localAgentInfo = agentState.getLocalAgentInfo();
        return Response
                .ok(localAgentInfo)
                .build();
    }

    @Consumes(MediaType.APPLICATION_JSON)
    @POST
    @ApiOperation(value = "Connect an agent to the master")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Return the agent status URL in the location header attribute"),
            @ApiResponse(code = 404, message = "Agent URL is invalid")
    })
    public Response connect(String connectURL) {
        agentState.connectTo(connectURL);
        StorageAgentInfo localAgentInfo = agentState.getLocalAgentInfo();
        return Response
                .created(resourceURI.getRequestUriBuilder()
                        .path("status")
                        .build())
                .entity(localAgentInfo)
                .build();
    }

}
