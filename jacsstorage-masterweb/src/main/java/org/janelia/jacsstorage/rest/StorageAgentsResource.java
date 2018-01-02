package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.service.distributedservice.StorageAgentManager;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agents")
@Api(value = "Agent registration API")
public class StorageAgentsResource {

    @Inject
    private StorageAgentManager agentManager;
    @Context
    private UriInfo resourceURI;

    @Path("url/{agentURL:.+}")
    @GET
    @ApiOperation(value = "Find registered agent")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return the agent info"),
            @ApiResponse(code = 404, message = "Agent URL is invalid")
    })
    public Response findRegisteredAgent(@PathParam("agentURL") String agentLocationInfo) {
        return agentManager.findRegisteredAgent(agentLocationInfo)
                .map(agentInfo -> Response.ok(agentInfo).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @GET
    @ApiOperation(value = "List registered agents")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return registered agents info")
    })
    public Response getCurrentRegisteredAgents(@QueryParam("connStatus") String connectionStatus) {
        return Response
                .ok(agentManager.getCurrentRegisteredAgents(ac -> StringUtils.isBlank(connectionStatus) || StringUtils.equals(connectionStatus, ac.getAgentInfo().getConnectionStatus())))
                .build();
    }

    @Consumes("application/json")
    @POST
    @ApiOperation(value = "Register/re-register agent")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "Return registered agent info")
    })
    public Response registerAgent(StorageAgentInfo agentInfo) {
        StorageAgentInfo registeterdAgentInfo = agentManager.registerAgent(agentInfo);
        return Response
                .created(resourceURI.getBaseUriBuilder().path("url/{agentURL:.+}").build(registeterdAgentInfo.getAgentHttpURL()))
                .entity(registeterdAgentInfo)
                .build();
    }

    @Path("url/{agentURL:.+}")
    @DELETE
    @ApiOperation(value = "Deregister agent")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "If the de-registration was successful"),
            @ApiResponse(code = 404, message = "Agent to be de-registered was not found or the token was invalid")
    })
    public Response deregisterAgent(@PathParam("agentURL") String agentURL, @HeaderParam("agentToken") String agentToken) {
        if (agentManager.deregisterAgent(agentURL, agentToken) != null) {
            return Response
                    .noContent()
                    .build();
        } else {
            return Response.status(Response.Status.NOT_FOUND).build();
        }
    }
}
