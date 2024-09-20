package org.janelia.jacsstorage.rest;

import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.helper.OriginalStorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.OriginalStoragePathURI;
import org.janelia.jacsstorage.model.jacsstorage.StorageRelativePath;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "Agent storage API on a particular volume")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class VolumeStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeStorageResource.class);

    @Inject
    private DataStorageService dataStorageService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject
    private AgentState agentState;
    @Context
    private UriInfo resourceURI;

    @ApiOperation(value = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The content was found"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @HEAD
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response checkDataPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                   @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                   @QueryParam("directoryOnly") Boolean directoryOnlyParam) {
        LOG.debug("Check data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            LOG.warn("Attempt to check {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath))
                .map(dataPath -> storageResourceHelper.checkContentFromFile(storageVolume, dataPath, directoryOnlyParam != null && directoryOnlyParam).build())
                .orElseGet(() -> Response
                        .status(Response.Status.BAD_REQUEST)
                        .header("Content-Length", 0)
                        .build())
                ;
    }

    @ApiOperation(value = "Stream the specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response retrieveDataContentFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                         @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                         @Context UriInfo requestURI) {
        LOG.debug("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume;
        try {
            storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        } catch (Exception e) {
            LOG.error("Error retrieving volume info for {}", storageVolumeId, e);
            return Response
                    .status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(new ErrorResponse("No volume info returned for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        ContentFilterParams filterParams = ContentFilterRequestHelper.createContentFilterParamsFromQuery(requestURI.getQueryParameters());
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.retrieveContentFromFile(storageVolume, filterParams, storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).orElse(null)).build();
        } else {
            LOG.warn("Attempt to read {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    @ApiOperation(value = "Stream content info of specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The operation  was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_info/{storageRelativePath:.+}")
    public Response retrieveDataInfoFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                      @PathParam("storageRelativePath") String storageRelativeFilePath) {
        LOG.debug("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.retrieveContentInfoFromFile(storageVolume, storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).orElse(null)).build();
        } else {
            LOG.warn("Attempt to get info about {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
    }

    @ApiOperation(value = "List the content of the specified path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The listing was successful"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_volume/{storageVolumeId}/list/{storageRelativePath:.*}")
    public Response listPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                              @PathParam("storageRelativePath") String storageRelativeFilePath,
                                              @QueryParam("depth") Integer depthParam,
                                              @QueryParam("offset") Long offsetParam,
                                              @QueryParam("length") Long lengthParam) {
        LOG.debug("Check data from volume {}:{} with a depthParameter {}", storageVolumeId, storageRelativeFilePath, depthParam);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .build();
        } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            LOG.warn("Attempt to list content of {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .build();
        }
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        long offset = offsetParam != null ? offsetParam : 0L;
        long length = lengthParam != null ? lengthParam : -1;
        return storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath))
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    Stream<DataNodeInfo> contentInfoStream;
                    if (length > 0) {
                        contentInfoStream = dataStorageService.streamDataEntries(dataEntryPath, "", storageFormat, depth)
                                .skip(offset > 0 ? offset : 0)
                                .limit(length);
                    } else {
                        contentInfoStream = dataStorageService.streamDataEntries(dataEntryPath, "", storageFormat, depth)
                                .skip(offset > 0 ? offset : 0);
                    }
                    return Response
                            .ok(contentInfoStream.peek(dn -> {
                                String dataNodeAbsolutePath = dataEntryPath.resolve(dn.getNodeRelativePath()).toString();
                                java.nio.file.Path dataNodeVolumeRelativePath = storageVolume.getPathRelativeToBaseStorageRoot(dataNodeAbsolutePath);
                                dn.setNumericStorageId(storageVolume.getId());
                                dn.setStorageRootLocation(storageVolume.getStorageVirtualPath());
                                dn.setStorageRootPathURI(storageVolume.getStorageURI());
                                if (storageFormat == JacsStorageFormat.SINGLE_DATA_FILE) {
                                    dn.setNodeRelativePath(dataNodeVolumeRelativePath.toString().replace('\\', '/'));
                                }
                                dn.setNodeAccessURL(resourceURI.getBaseUriBuilder()
                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                        .path("storage_volume")
                                        .path(storageVolume.getId().toString())
                                        .path("data_content")
                                        .path(dataNodeVolumeRelativePath.toString())
                                        .build()
                                        .toString()
                                );
                                dn.setNodeInfoURL(resourceURI.getBaseUriBuilder()
                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                        .path("storage_volume")
                                        .path(storageVolume.getId().toString())
                                        .path("data_info")
                                        .path(dataNodeVolumeRelativePath.toString())
                                        .build()
                                        .toString()
                                );
                            }).collect(Collectors.toList()),
                                    MediaType.APPLICATION_JSON)
                            .build();
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND).build())
                ;
    }

    @ApiOperation(value = "Store the content on the specified volume at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The data was saved successfully"),
            @ApiResponse(code = 404, message = "Invalid volume identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data write error")
    })
    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1}
    )
    @RequireAuthentication
    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response storeDataContentOnStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                    @PathParam("storageRelativePath") String storageRelativePathParam,
                                                    @Context SecurityContext securityContext,
                                                    InputStream contentStream) {
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.WRITE)) {
            OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.storeFileContent(storageVolume,
                    resourceURI.getBaseUri(),
                    storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativePathParam)).orElse(null),
                    contentStream)
                    .build();
        } else {
            LOG.warn("Attempt to write {} on volume {} but the volume does not allow WRITE", storageRelativePathParam, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No write permission for volume " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }

    }

    @ApiOperation(value = "Delete specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(code = 204, message = "The delete was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or invalid file path"),
            @ApiResponse(code = 500, message = "Data delete error")
    })
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response deleteDataContentFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                       @PathParam("storageRelativePath") String storageRelativeFilePath) {
        LOG.debug("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.warn("No accessible volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.DELETE)) {
            OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.removeFileContentFromVolume(
                    storageVolume,
                    storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).orElse(null)
            ).build();
        } else {
            LOG.warn("Attempt to read {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
    }

    @ApiOperation(
            value = "List storage volumes. The volumes could be filtered by {id, storageHost, storageTags, volumeName}."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The list of storage entries that match the given filters",
                    response = JacsStorageVolume.class
            ),
            @ApiResponse(
                    code = 401,
                    message = "If user is not authenticated",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 500,
                    message = "Data read error",
                    response = ErrorResponse.class
            )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_volumes")
    public Response listStorageVolumes(@QueryParam("id") Long storageVolumeId,
                                       @QueryParam("name") String volumeName,
                                       @QueryParam("shared") boolean shared,
                                       @QueryParam("storageTags") List<String> storageTags,
                                       @QueryParam("storageVirtualPath") String storageVirtualPath,
                                       @QueryParam("dataStoragePath") String dataStoragePathParam,
                                       @QueryParam("includeInactive") boolean includeInactive,
                                       @QueryParam("includeInaccessibleVolumes") boolean includeInaccessibleVolumes,
                                       @Context SecurityContext securityContext) {
        StorageQuery storageQuery = new StorageQuery()
                .setId(storageVolumeId)
                .setStorageName(volumeName)
                .setShared(shared)
                .setAccessibleOnAgent(agentState.getLocalAgentId())
                .setStorageTags(storageTags)
                .setStorageVirtualPath(storageVirtualPath)
                .setDataStoragePath(OriginalStoragePathURI.createAbsolutePathURI(dataStoragePathParam).getStoragePath())
                .setIncludeInactiveVolumes(includeInactive)
                .setIncludeInaccessibleVolumes(includeInaccessibleVolumes);
        LOG.info("List storage volumes filtered with: {}", storageQuery);
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(storageQuery);
        PageResult<JacsStorageVolume> results = new PageResult<>();
        results.setPageSize(storageVolumes.size());
        results.setResultList(storageVolumes);
        return Response
                .ok(results)
                .build();
    }

}
