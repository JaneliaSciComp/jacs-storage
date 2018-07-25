package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.net.URI;

@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
@Api(value = "Agent storage API based on file's path. This API requires an authenticated subject.")
public class PathBasedAgentStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(PathBasedAgentStorageResource.class);

    @Inject
    private DataStorageService dataStorageService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Context
    private UriInfo resourceURI;

    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @HEAD
    @Path("storage_path/{dataPath:.*}")
    @ApiOperation(value = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The content was found"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response checkPath(@PathParam("dataPath") String fullDataPathNameParam) {
        LOG.info("Check path {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> Response
                        .ok(),
                (storageVolume, dataEntryPath) -> storageResourceHelper.checkContentFromFile(storageVolume, dataEntryPath)
        ).build();
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
    public Response retrieveData(@PathParam("dataPath") String fullDataPathNameParam) {
        LOG.info("Retrieve data from {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> storageResourceHelper.retrieveContentFromDataBundle(dataBundle, dataEntryPath),
                (storageVolume, dataEntryPath) -> storageResourceHelper.retrieveContentFromFile(storageVolume, dataEntryPath)
        ).build();
    }

    @LogStorageEvent(
            eventName = "DELETE_STORAGE_ENTRY",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Produces({MediaType.APPLICATION_JSON})
    @DELETE
    @Path("storage_path/{dataPath:.*}")
    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response removeData(@PathParam("dataPath") String fullDataPathNameParam, @Context SecurityContext securityContext) {
        LOG.info("Remove data from {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> storageResourceHelper.removeContentFromDataBundle(dataBundle,
                        dataEntryPath,
                        (Long freedEntrySize) -> storageAllocatorService.updateStorage(
                                new JacsBundleBuilder()
                                        .dataBundleId(dataBundle.getId())
                                        .usedSpaceInBytes(-freedEntrySize)
                                        .build(),
                                SecurityUtils.getUserPrincipal(securityContext))),
                (storageVolume, dataEntryPath) -> storageResourceHelper.removeFileContentFromVolume(storageVolume, dataEntryPath)
        ).build();
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Produces({MediaType.APPLICATION_JSON})
    @PUT
    @Path("storage_path/file/{dataPath:.*}")
    @ApiOperation(value = "Store the content at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The data was saved successfully"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data write error")
    })
    public Response storeData(@PathParam("dataPath") String fullDataPathNameParam, @Context SecurityContext securityContext, InputStream contentStream) {
        LOG.info("Retrieve data from {}", fullDataPathNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) ->
                        storageResourceHelper.storeDataBundleContent(dataBundle,
                                resourceURI.getBaseUri(),
                                dataEntryPath,
                                (Long newEntrySize) -> storageAllocatorService.updateStorage(
                                            new JacsBundleBuilder()
                                                    .dataBundleId(dataBundle.getId())
                                                    .usedSpaceInBytes(newEntrySize)
                                                    .build(),
                                            SecurityUtils.getUserPrincipal(securityContext)),
                                contentStream),
                (storageVolume, dataEntryPath) ->
                        storageResourceHelper.storeFileContent(storageVolume,
                                resourceURI.getBaseUri(),
                                dataEntryPath,
                                contentStream)
        ).build();
    }

    @RequireAuthentication
    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("storage_path/list/{dataPath:.*}")
    @ApiOperation(
            value = "List the content.",
            notes = "Lists tree hierarchy of the storage path"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully read the data bundle content."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response listContent(@PathParam("dataPath") String fullDataPathNameParam,
                                @QueryParam("depth") Integer depthParam,
                                @Context SecurityContext securityContext) {
        LOG.info("List content from location {} with a depthParameter {}", fullDataPathNameParam, depthParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        URI baseURI = resourceURI.getBaseUri();
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullDataPathNameParam,
                (dataBundle, dataEntryPath) -> storageResourceHelper.listContentFromDataBundle(dataBundle, baseURI, dataEntryPath, depth),
                (storageVolume, dataEntryPath) -> storageResourceHelper.listContentFromPath(storageVolume, baseURI, dataEntryPath, depth)
        ).build();

    }

    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @HEAD
    @Path("storage_volume/{storageVolumeId}/{storageRelativePath:.+}")
    @ApiOperation(value = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The content was found"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response checkPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                               @PathParam("storageRelativePath") String storageRelativeFilePath) {
        LOG.info("Check data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + storageVolumeId))
                    .build();
        } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.checkContentFromFile(storageVolume, storageRelativeFilePath).build();
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
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.retrieveContentFromFile(storageVolume, storageRelativeFilePath).build();
        } else {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
    }
}
