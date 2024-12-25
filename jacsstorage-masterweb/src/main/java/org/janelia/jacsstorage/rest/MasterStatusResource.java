package org.janelia.jacsstorage.rest;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;


@Tag(name = "MasterStatus", description = "Master storage status API.")
@Path("storage")
public class MasterStatusResource {

    @Operation(description = "Retrieve master status.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "If the server is up and running")
    })
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("status")
    public String getStatus() {
        return "OK";
    }

}
