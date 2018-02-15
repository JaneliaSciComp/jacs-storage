package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageContentReader;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

@Timed
@RequestScoped
@RequireAuthentication
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
@Api(value = "Agent storage API based on file's path. This API requires an authenticated subject.")
public class PathBasedAgentStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(PathBasedAgentStorageResource.class);

    @Inject
    private StorageContentReader storageContentReader;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Context
    private UriInfo resourceURI;

    @HEAD
    @Path("storage_path/{filePath:.*}")
    @ApiOperation(value = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The content was found"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response checkPath(@PathParam("filePath") String fullDataPathNameParam,
                              @Context SecurityContext securityContext) {
        LOG.info("Check path {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageContentReader, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> Response
                        .ok()
                        .build(),
                (storageVolume, dataEntryPath) -> storageResourceHelper.checkContentFromFile(storageVolume, dataEntryPath)
        );
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_path/{dataPath:.*}")
    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveData(@PathParam("dataPath") String fullDataPathNameParam,
                                 @Context SecurityContext securityContext) {
        LOG.info("Retrieve data from {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageContentReader, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> storageResourceHelper.retrieveContentFromDataBundle(dataBundle, dataEntryPath),
                (storageVolume, dataEntryPath) -> storageResourceHelper.retrieveContentFromFile(storageVolume, dataEntryPath)
        );
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_volume/{storageVolumeId}/{storageRelativePath:.+}")
    @ApiOperation(value = "Stream the specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveDataFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                  @PathParam("storageRelativePath") String storageRelativeFilePath) {
        LOG.info("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + storageVolumeId))
                    .build();
        }
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageContentReader, storageLookupService, storageVolumeManager);
        return storageResourceHelper.retrieveContentFromFile(storageVolume, storageRelativeFilePath);
    }

}
