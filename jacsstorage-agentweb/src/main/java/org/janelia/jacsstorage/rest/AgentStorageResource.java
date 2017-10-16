package org.janelia.jacsstorage.rest;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.service.DataStorageService;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agent-storage")
public class AgentStorageResource {

    @Inject
    private DataStorageService dataStorageService;

    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @POST
    @Path("{storageFormat}/{dataPath}")
    public Response persistStream(@PathParam("storageFormat") JacsStorageFormat storageFormat,
                                  @PathParam("dataPath") String dataPath,
                                  InputStream bundleStream) throws IOException {
        TransferInfo ti = dataStorageService.persistDataStream(dataPath, storageFormat, bundleStream);
        return Response
                .ok(ti)
                .build();
    }

    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @GET
    @Path("{storageFormat}/{dataPath}")
    public Response retrieveStream(@PathParam("storageFormat") JacsStorageFormat storageFormat,
                                  @PathParam("dataPath") String dataPath) {
        StreamingOutput bundleStream =  new StreamingOutput()
        {
            @Override
            public void write(java.io.OutputStream output) throws IOException, WebApplicationException {
                try {
                    dataStorageService.retrieveDataStream(dataPath, storageFormat, output);
                    output.flush();
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataPath)
                .build();
    }

    @DELETE
    public Response deleteStorage(@QueryParam("dataPath") String dataPath, @QueryParam("parentPath") String parentPath) throws IOException {
        if (StringUtils.isBlank(dataPath)) {
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .build();
        }
        dataStorageService.deleteStorage(dataPath);
        if (StringUtils.isNotBlank(parentPath)) dataStorageService.cleanupStorage(parentPath);
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
