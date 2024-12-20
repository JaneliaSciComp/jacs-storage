package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.service.impl.distributedservice.StorageAgentManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "StorageAgents", description = "Agent registration API")
@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agents")
public class StorageAgentsResource {
    private static final Logger LOG = LoggerFactory.getLogger(StorageAgentsResource.class);

    @Inject
    private StorageAgentManager agentManager;
    @Context
    private UriInfo resourceURI;

    @Operation(description = "Find registered agent")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Return the agent info"),
            @ApiResponse(responseCode = "404", description = "Agent URL is invalid")
    })
    @GET
    @Path("url/{agentURL:.+}")
    public Response findRegisteredAgent(@PathParam("agentURL") String agentLocationInfo) {
        LOG.trace("Find registered agent for {}", agentLocationInfo);
        return agentManager.findRegisteredAgent(agentLocationInfo)
                .map(agentInfo -> Response.ok(agentInfo).build())
                .orElseGet(() -> Response.status(Response.Status.NOT_FOUND).build());
    }

    @Operation(description = "List registered agents")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Return registered agents info")
    })
    @GET
    public Response getCurrentRegisteredAgents(@QueryParam("connStatus") String connectionStatus) {
        LOG.debug("Get available agents with connectionStatus - {}", StringUtils.defaultIfBlank(connectionStatus, "<ANY>"));
        return Response
                .ok(agentManager.getCurrentRegisteredAgents(ac -> StringUtils.isBlank(connectionStatus) || StringUtils.equals(connectionStatus, ac.getAgentInfo().getConnectionStatus())))
                .build();
    }

    @Operation(description = "Register/re-register agent")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "202", description = "Return registered agent info")
    })
    @Timed
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response registerAgent(StorageAgentInfo agentInfo) {
        LOG.info("Register agent - {}", agentInfo);
        StorageAgentInfo registeterdAgentInfo = agentManager.registerAgent(agentInfo);
        return Response
                .created(resourceURI.getBaseUriBuilder().path("url/{agentURL:.+}").build(registeterdAgentInfo.getAgentAccessURL()))
                .entity(registeterdAgentInfo)
                .build();
    }

    @Operation(description = "Deregister agent")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "If the de-registration was successful"),
            @ApiResponse(responseCode = "404", description = "Agent to be de-registered was not found or the token was invalid")
    })
    @Timed
    @DELETE
    @Path("url/{agentURL:.+}")
    public Response deregisterAgent(@PathParam("agentURL") String agentURL, @HeaderParam("agentToken") String agentToken) {
        LOG.info("Disconnect agent - {} with {}", agentURL, agentToken);
        if (agentManager.deregisterAgent(agentURL, agentToken) != null) {
            LOG.info("Disconnected agent {} with {}", agentURL, agentToken);
            return Response
                    .noContent()
                    .build();
        } else {
            LOG.warn("No connected agent found for {} with {}", agentURL, agentToken);
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
