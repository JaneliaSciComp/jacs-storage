package org.janelia.jacsstorage.rest;

import javax.enterprise.context.RequestScoped;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@RequestScoped
@Path("/storage")
public class StorageResource {

    @Produces(MediaType.TEXT_PLAIN)
    @GET
    public String get1() {
        return "OK 1";
    }

    @Produces(MediaType.TEXT_PLAIN)
    @GET
    @Path("tttt")
    public String get2() {
        return "OK 2";
    }
}
