package org.janelia.jacsstorage.rest;

import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
public class StorageResource {

    @GET
    @Path("status")
    public String getStatus() {
        return "OK";
    }

}
