package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.model.jacsstorage.StorageAgentInfo;
import org.janelia.jacsstorage.service.StorageAgentManager;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@ApplicationScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agents")
public class StorageAgentsResource {

    @Inject
    private StorageAgentManager agentManager;

    @GET
    public Response getCurrentRegisteredAgents() {
        return Response
                .ok(agentManager.getCurrentRegisteredAgents())
                .build();
    }

    @Consumes("application/json")
    @POST
    public Response registerAgent(StorageAgentInfo agentInfo) {
        agentManager.registerAgent(agentInfo);
        return Response
                .ok(agentInfo)
                .build();
    }

    @Path("{connInfo}")
    @DELETE
    public Response deregisterAgent(@PathParam("connInfo") String connectionInfo) {
        agentManager.deregisterAgent(connectionInfo);
        return Response
                .noContent()
                .build();
    }
}
