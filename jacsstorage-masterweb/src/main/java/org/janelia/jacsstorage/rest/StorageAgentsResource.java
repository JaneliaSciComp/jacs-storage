package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.StorageAgentInfo;
import org.janelia.jacsstorage.service.distributedservice.StorageAgentManager;

import javax.annotation.security.PermitAll;
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
public class StorageAgentsResource {

    @Inject
    private StorageAgentManager agentManager;
    @Context
    private UriInfo resourceURI;

    @PermitAll
    @Path("url/{agentURL: .+}")
    @GET
    public Response findRegisteredAgent(@PathParam("agentURL") String agentLocationInfo) {
        return agentManager.findRegisteredAgent(agentLocationInfo)
                .map(agentInfo -> Response.ok(agentInfo).build())
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @PermitAll
    @GET
    public Response getCurrentRegisteredAgents(@QueryParam("connStatus") String connectionStatus) {
        return Response
                .ok(agentManager.getCurrentRegisteredAgents(ac -> StringUtils.equals(connectionStatus, ac.getAgentInfo().getConnectionStatus())))
                .build();
    }

    @PermitAll
    @Consumes("application/json")
    @POST
    public Response registerAgent(StorageAgentInfo agentInfo) {
        StorageAgentInfo registeterdAgentInfo = agentManager.registerAgent(agentInfo);
        return Response
                .created(resourceURI.getBaseUriBuilder().path("url/{agentURL: .+}").build(registeterdAgentInfo.getAgentHttpURL()))
                .entity(registeterdAgentInfo)
                .build();
    }

    @PermitAll
    @Path("url/{agentURL: .+}")
    @DELETE
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
