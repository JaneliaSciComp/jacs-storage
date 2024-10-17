package org.janelia.jacsstorage.rest;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.service.N5ContentService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.saalfeldlab.n5.N5TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "Agent storage API for N5 file structures")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class N5StorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(N5StorageResource.class);

    @Inject
    @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject
    private N5ContentService n5ContentService;

    @ApiOperation(value = "Discover N5 data sets in the given path and return a tree of N5TreeNodes")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The operation was successful"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/n5tree/{storageRelativePath:.+}")
    public Response retrieveDataInfoFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                      @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                      @Context ContainerRequestContext requestContext) {
        LOG.debug("Retrieve N5 data sets from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            JADEStorageOptions storageOptions = new JADEStorageOptions()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"));
            JADEStorageURI n5ContainerURI = storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .orElse(null);
            if (n5ContainerURI == null) {
                return Response
                        .serverError()
                        .entity(ImmutableMap.of("errormessage", "Could not resolve relative path: " + storageRelativeFilePath))
                        .build();
            }
            N5TreeNode n5RootNode = n5ContentService.getN5Container(n5ContainerURI);
            return Response
                    .ok(n5RootNode, MediaType.APPLICATION_JSON)
                    .build();
        } else {
            LOG.warn("Attempt to get info about {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
    }
}
