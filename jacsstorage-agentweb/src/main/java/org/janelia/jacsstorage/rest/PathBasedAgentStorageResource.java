package org.janelia.jacsstorage.rest;

import java.io.InputStream;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

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
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.OriginalStorageResourceHelper;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.OriginalStoragePathURI;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.OriginalDataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "Agent storage API based on file's path.")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class PathBasedAgentStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(PathBasedAgentStorageResource.class);

    @Inject
    private DataContentService dataContentService;
    @Inject
    private OriginalDataStorageService originalDataStorageService;
    @Inject
    @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject
    @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject
    @LocalInstance
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
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response checkPath(@PathParam("dataPath") String dataPathParam, @QueryParam("directoryOnly") Boolean directoryOnlyParam) {
        try {
            LOG.debug("Start check path {}", dataPathParam);
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam);
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
            List<JacsStorageVolume> volumeCandidates;
            try {
                volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
            } catch (IllegalArgumentException e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse(e.getMessage()))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            if (CollectionUtils.isEmpty(volumeCandidates)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            List<JacsStorageVolume> accessibleVolumes = volumeCandidates.stream()
                    .filter(storageVolume -> storageVolume.hasPermission(JacsStoragePermission.READ))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(accessibleVolumes)) {
                return Response.status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No permissions to access " + contentURI))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            return accessibleVolumes.stream()
                    .findFirst()
                    .flatMap(aStorageVolume -> aStorageVolume.resolveDataContentURI(contentURI))
                    .map(resolvedContentURI -> {
                        List<ContentNode> contentNodes = dataContentService.listDataNodes(resolvedContentURI, new ContentFilterParams());
                        if (contentNodes.isEmpty()) {
                            return Response.status(Response.Status.NOT_FOUND)
                                    .header("Content-Length", 0);
                        } else {
                            return Response.ok()
                                    .header("Content-Length", 0);
                        }
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND)
                            .header("Content-Length", 0))
                    .build();
        } finally {
            LOG.debug("Complete check path {}", dataPathParam);
        }
    }

    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response retrieveContent(@PathParam("dataPath") String dataPathParam,
                                    @Context UriInfo requestURI) {
        try {
            LOG.debug("Start retrieve data from {}", dataPathParam);
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam);
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
            List<JacsStorageVolume> volumeCandidates;
            try {
                volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
            } catch (IllegalArgumentException e) {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(new ErrorResponse(e.getMessage()))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            if (CollectionUtils.isEmpty(volumeCandidates)) {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            List<JacsStorageVolume> accessibleVolumes = volumeCandidates.stream()
                    .filter(storageVolume -> storageVolume.hasPermission(JacsStoragePermission.READ))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(accessibleVolumes)) {
                return Response.status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No permissions to access " + contentURI))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            ContentFilterParams filterParams = ContentFilterRequestHelper.createContentFilterParamsFromQuery(requestURI.getQueryParameters());
            return accessibleVolumes.stream()
                    .findFirst()
                    .flatMap(aStorageVolume -> aStorageVolume.resolveDataContentURI(contentURI))
                    .map(resolvedContentURI -> {
                        StreamingOutput outputStream = output -> {
                            dataContentService.readDataStream(resolvedContentURI, filterParams, output);
                            output.flush();
                        };
                        return Response
                                .ok(outputStream, MediaType.APPLICATION_OCTET_STREAM)
                                .header("Content-Length", null)
                                .header("Content-Disposition", "attachment; filename = " + resolvedContentURI.getStorageObjectName())
                                ;
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND)
                            .header("Content-Length", 0))
                    .build();
        } finally {
            LOG.debug("Complete retrieve data from {}", dataPathParam);
        }
    }

    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The data streaming was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @LogStorageEvent(
            eventName = "DELETE_STORAGE_ENTRY",
            argList = {0, 1}
    )
    @RequireAuthentication
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response removeData(@PathParam("dataPath") String dataPathParam, @Context SecurityContext securityContext) {
        LOG.debug("Remove data from {}", dataPathParam);
        OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(originalDataStorageService, storageLookupService, storageVolumeManager);
        JacsCredentials credentials = SecurityUtils.getUserPrincipal(securityContext);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                OriginalStoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) -> storageResourceHelper.removeContentFromDataBundle(dataBundle,
                        dataEntryName,
                        credentials,
                        (Long freedEntrySize) -> storageAllocatorService.updateStorage(
                                new JacsBundleBuilder()
                                        .dataBundleId(dataBundle.getId())
                                        .usedSpaceInBytes(-freedEntrySize)
                                        .build(),
                                credentials)),
                (storageVolume, storageDataPathURI) -> storageResourceHelper.removeFileContentFromVolume(storageVolume, Paths.get(storageDataPathURI.getStoragePath()))
        ).build();
    }

    @ApiOperation(value = "Store the content at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The data was saved successfully"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
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
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response storeData(@PathParam("dataPath") String dataPathParam, @Context SecurityContext securityContext, InputStream contentStream) {
        LOG.debug("Retrieve data from {}", dataPathParam);
        OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(originalDataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                OriginalStoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) ->
                        storageResourceHelper.storeDataBundleContent(dataBundle,
                                resourceURI.getBaseUri(),
                                dataEntryName,
                                (Long newEntrySize) -> storageAllocatorService.updateStorage(
                                        new JacsBundleBuilder()
                                                .dataBundleId(dataBundle.getId())
                                                .usedSpaceInBytes(newEntrySize)
                                                .build(),
                                        SecurityUtils.getUserPrincipal(securityContext)),
                                contentStream),
                (storageVolume, storageDataPathURI) ->
                        storageResourceHelper.storeFileContent(storageVolume,
                                resourceURI.getBaseUri(),
                                Paths.get(storageDataPathURI.getStoragePath()),
                                contentStream)
        ).build();
    }

    @ApiOperation(value = "Inspect and retrieve content info of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The operation was successfull. " +
                    "As a note the operation is considered successful even if there's currently no process to extract any information from the content"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @RequireAuthentication
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path/data_info/{dataPath:.+}")
    public Response retrieveDataInfo(@PathParam("dataPath") String dataPathParam) {
        LOG.debug("Retrieve data info from {}", dataPathParam);
        OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(originalDataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                OriginalStoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryPath) -> storageResourceHelper.retrieveContentInfoFromDataBundle(dataBundle, dataEntryPath),
                (storageVolume, storageDataPathURI) -> storageResourceHelper.retrieveContentInfoFromFile(storageVolume, Paths.get(storageDataPathURI.getStoragePath()))
        ).build();
    }

    @ApiOperation(
            value = "List the content.",
            notes = "Lists tree hierarchy of the storage path"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully read the data bundle content."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @RequireAuthentication
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_path/list/{dataPath:.*}")
    public Response listContent(@PathParam("dataPath") String dataPathParam,
                                @QueryParam("depth") Integer depthParam,
                                @QueryParam("offset") Integer offsetParam,
                                @QueryParam("length") Integer lengthParam,
                                @Context SecurityContext securityContext) {
        LOG.debug("List content from location {} with a depthParameter {}", dataPathParam, depthParam);
        OriginalStorageResourceHelper storageResourceHelper = new OriginalStorageResourceHelper(originalDataStorageService, storageLookupService, storageVolumeManager);
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        long offset = offsetParam != null ? offsetParam : 0;
        long length = lengthParam != null ? lengthParam : -1;
        URI baseURI = resourceURI.getBaseUri();
        return storageResourceHelper.handleResponseForFullDataPathParam(
                OriginalStoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) -> storageResourceHelper.listContentFromDataBundle(dataBundle, baseURI, dataEntryName, depth, offset, length),
                (storageVolume, storageDataPathURI) -> storageResourceHelper.listContentFromPath(storageVolume, baseURI, Paths.get(storageDataPathURI.getStoragePath()), depth, offset, length)
        ).build();
    }

}
