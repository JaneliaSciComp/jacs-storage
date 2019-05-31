package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StorageRelativePath;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.nio.file.Files;
import java.util.List;

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
        LOG.info("Check data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.error("No volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .header("Content-Length", 0)
                    .build();
        } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            LOG.warn("Attempt to check {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .header("Content-Length", 0)
                    .build();
        }
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.checkContentFromFile(storageVolume, StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath), directoryOnlyParam != null && directoryOnlyParam).build();
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
        LOG.info("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.error("No volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + storageVolumeId))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        ContentFilterParams filterParams = ContentFilterRequestHelper.createContentFilterParamsFromQuery(requestURI.getQueryParameters());
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.retrieveContentFromFile(storageVolume, filterParams, StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).build();
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
        LOG.info("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            LOG.error("No volume found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + storageVolumeId))
                    .build();
        }
        if (storageVolume.hasPermission(JacsStoragePermission.READ)) {
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
            return storageResourceHelper.retrieveContentInfoFromFile(storageVolume, StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath)).build();
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
    @Path("storage_volume/{storageVolumeId}/list/{storageRelativePath:.+}")
    public Response listPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                              @PathParam("storageRelativePath") String storageRelativeFilePath,
                                              @QueryParam("depth") Integer depthParam) {
        LOG.info("Check data from volume {}:{} with a depthParameter {}", storageVolumeId, storageRelativeFilePath, depthParam);
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        return storageVolume.getDataStorageAbsolutePath(StorageRelativePath.pathRelativeToBaseRoot(storageRelativeFilePath))
                .filter(dataEntryPath -> Files.exists(dataEntryPath))
                .map(dataEntryPath -> {
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    List<DataNodeInfo> contentInfoList = dataStorageService.listDataEntries(dataEntryPath, "", storageFormat, depth);
                    contentInfoList.forEach(dn -> {
                        String dataNodeAbsolutePath = dataEntryPath.resolve(dn.getNodeRelativePath()).toString();
                        java.nio.file.Path dataNodeVolumeRelativePath = storageVolume.getPathRelativeToBaseStorageRoot(dataNodeAbsolutePath);
                        dn.setNumericStorageId(storageVolume.getId());
                        dn.setStorageRootLocation(storageVolume.getStorageVirtualPath());
                        dn.setStorageRootPathURI(storageVolume.getStorageURI());
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
                    });
                    return Response
                            .ok(contentInfoList, MediaType.APPLICATION_JSON)
                            .build();
                })
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND).build())
                ;
    }
}
