package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.service.StorageServiceCoordinator;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
public class StorageResource {

    @Inject
    private StorageServiceCoordinator storageService;

    @GET
    @Path("status")
    public String getStatus() {
        return "OK";
    }

    @GET
    @Path("{id}")
    public Response getBundleInfo(@PathParam("id") Long id) {
        JacsBundle jacsBundle = storageService.getDataBundleById(id);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .ok(jacsBundle)
                    .build();
        }
    }

    @Consumes("application/json")
    @POST
    public Response createBundleInfo(JacsBundle dataBundle) {
        // TODO
        return Response
                .ok() // FIXME
                .build();

    }
}
