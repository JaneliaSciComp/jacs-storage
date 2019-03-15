package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Api(value = "Master storage status API.")
@ApplicationScoped
@Path("storage")
public class MasterStatusResource {

    @ApiOperation(value = "Retrieve master status.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "If the server is up and running")
    })
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("status")
    public String getStatus() {
        return "OK";
    }

}
