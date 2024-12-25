package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "AgentConnection", description = "Agent connectivity API")
@Path("connection")
public class AgentConnectionResource {
    private static final Logger LOG = LoggerFactory.getLogger(AgentConnectionResource.class);

    @Inject
    private AgentState agentState;
    @Context
    private UriInfo resourceURI;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("status")
    public Response getStatus() {
        StorageAgentInfo localAgentInfo = agentState.getLocalAgentInfo();
        return Response
                .ok(localAgentInfo)
                .build();
    }

    @Operation(description = "Connect this agent to the specified master URL")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Return this agent's connection status URL in the location header attribute"),
            @ApiResponse(responseCode = "404", description = "Master URL is invalid")
    })
    @Timed
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @POST
    public Response connect(String connectURL) {
        LOG.info("Connect agent to {}", connectURL);
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
