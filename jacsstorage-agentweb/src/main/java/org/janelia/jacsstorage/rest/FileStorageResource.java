package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class FileStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(FileStorageResource.class);

    @Inject
    private DataStorageService dataStorageService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Context
    private UriInfo resourceURI;

    @RequireAuthentication
    @HEAD
    @Path("path/{filePath:.*}")
    public Response checkPath(@PathParam("filePath") String fullFileName,
                              @Context SecurityContext securityContext,
                              InputStream contentStream) {
        java.nio.file.Path filePath = Paths.get(fullFileName);
        if (Files.notExists(filePath)) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path exists for " + fullFileName))
                    .build();
        } else {
            return Response
                    .ok()
                    .build();
        }
    }

    @RequireAuthentication
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @GET
    @Path("path/{dataPath:.*}")
    public Response retrieveData(@PathParam("dataPath") String fullDataPath,
                                 @Context SecurityContext securityContext,
                                 InputStream contentStream) {
        java.nio.file.Path dataPath = Paths.get(fullDataPath);
        if (Files.notExists(dataPath)) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path exists for " + fullDataPath))
                    .build();
        }
        JacsStorageFormat storageFormat = Files.isRegularFile(dataPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        StreamingOutput fileStream = output -> {
            try {
                dataStorageService.retrieveDataStream(dataPath, storageFormat, output);
                output.flush();
            } catch (Exception e) {
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataPath.toFile().getName())
                .build();
    }

}
