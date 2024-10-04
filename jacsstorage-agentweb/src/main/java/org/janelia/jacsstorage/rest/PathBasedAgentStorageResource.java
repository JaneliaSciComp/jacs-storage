package org.janelia.jacsstorage.rest;

import java.io.InputStream;
import java.net.URI;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.requesthelpers.ContentAccessRequestHelper;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.DataContentService;
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
    public Response checkPath(@PathParam("dataPath") String dataPathParam,
                              @QueryParam("directoryOnly") Boolean directoryOnlyParam) {
        try {
            LOG.debug("Start check path {}", dataPathParam);
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam);
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
                    .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI))
                    .map(resolvedContentURI -> {
                        boolean dataFound = dataContentService.exists(resolvedContentURI);
                        if (dataFound) {
                            return Response.ok();
                        } else {
                            return Response.status(Response.Status.NOT_FOUND);
                        }
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
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
            ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
            return accessibleVolumes.stream()
                    .findFirst()
                    .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI))
                    .map(resolvedContentURI -> {
                        ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                        long contentSize = contentGetter.estimateContentSize();
                        StreamingOutput outputStream = output -> {
                            contentGetter.streamContent(output);
                            output.flush();
                        };
                        return Response
                                .ok(outputStream, MediaType.APPLICATION_OCTET_STREAM)
                                .header("Content-Length", contentSize)
                                .header("Content-Disposition", "attachment; filename = " + resolvedContentURI.getObjectName())
                                ;
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
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
        try {
            LOG.debug("Remove data from {}", dataPathParam);
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
                    .filter(storageVolume -> storageVolume.hasPermission(JacsStoragePermission.DELETE))
                    .collect(Collectors.toList());
            if (CollectionUtils.isEmpty(accessibleVolumes)) {
                return Response.status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No permissions to delete " + contentURI))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            return accessibleVolumes.stream()
                    .findFirst()
                    .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI))
                    .map(resolvedContentURI -> {
                        dataContentService.removeData(resolvedContentURI);
                        return Response.noContent();
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
                    .build();
        } finally {
            LOG.debug("Complete remove data from {}", dataPathParam);
        }
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
                .filter(storageVolume -> storageVolume.hasPermission(JacsStoragePermission.WRITE))
                .collect(Collectors.toList());
        if (CollectionUtils.isEmpty(accessibleVolumes)) {
            return Response.status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No permissions to access " + contentURI))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return accessibleVolumes.stream()
                .findFirst()
                .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI)
                        .map(resolvedContentURI -> Pair.of(aStorageVolume, resolvedContentURI)))
                .map(volAndContentURIPair -> {
                    JacsStorageVolume storageVolume = volAndContentURIPair.getLeft();
                    JADEStorageURI resolvedContentURI = volAndContentURIPair.getRight();
                    long size = dataContentService.writeDataStream(resolvedContentURI, contentStream);
                    URI newContentURI = UriBuilder.fromUri(resourceURI.getBaseUri())
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path("storage_path/data_content")
                            .path(resolvedContentURI.getJadeStorage())
                            .build();
                    DataNodeInfo newContentNode = new DataNodeInfo();
                    newContentNode.setStorageRootLocation(storageVolume.getStorageRootLocation());
                    newContentNode.setNodeInfoURL(UriBuilder.fromUri(resourceURI.getBaseUri())
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path("storage_path/data_info")
                            .path(resolvedContentURI.getJadeStorage())
                            .build()
                            .toString()
                    );
                    newContentNode.setNodeAccessURL(newContentURI.toString());
                    newContentNode.setNodeRelativePath(storageVolume.getContentRelativePath(resolvedContentURI));
                    newContentNode.setSize(size);
                    newContentNode.setCollectionFlag(false);
                    return Response.created(newContentURI).entity(newContentNode);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND))
                .build();
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
    public Response retrieveContentMetadata(@PathParam("dataPath") String dataPathParam, @Context UriInfo requestURI) {
        try {
            LOG.debug("Retrieve metadata from {}", dataPathParam);
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
            ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
            return accessibleVolumes.stream()
                    .findFirst()
                    .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI))
                    .map(resolvedContentURI -> {
                        ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                        return Response.ok(contentGetter.getMetaData());
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
                    .build();
        } finally {
            LOG.debug("Complete retrieve metadata from {}", dataPathParam);
        }
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
                                @Context UriInfo requestURI,
                                @Context SecurityContext securityContext) {
        LOG.debug("List content from location {} with a depthParameter {}", dataPathParam, depthParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam);
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        int offset = offsetParam != null ? offsetParam : 0;
        int length = lengthParam != null ? lengthParam : -1;
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
                    .entity(new ErrorResponse("No permissions to read " + contentURI))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        URI endpointBaseURI = resourceURI.getBaseUri();
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters())
                .setMaxDepth(depth)
                .setEntriesCount(length)
                .setStartEntryIndex(offset);
        return accessibleVolumes.stream()
                .findFirst()
                .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI)
                        .map(resolvedContentURI -> Pair.of(aStorageVolume, resolvedContentURI)))
                .map(volAndContentURIPair -> {
                    JacsStorageVolume storageVolume = volAndContentURIPair.getLeft();
                    JADEStorageURI resolvedContentURI = volAndContentURIPair.getRight();
                    JADEStorageURI storageVolumeURI = storageVolume.getVolumeStorageRootURI();
                    ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                    List<DataNodeInfo> dataNodes = contentGetter.getObjectsList().stream()
                            .map(contentNode -> {
                                DataNodeInfo dataNode = new DataNodeInfo();
                                dataNode.setStorageRootBinding(storageVolumeURI.getJadeStorage());
                                dataNode.setNodeRelativePath(storageVolumeURI.relativizeKey(contentNode.getObjectKey()));
                                dataNode.setMimeType(contentNode.getMimeType());
                                dataNode.setSize(contentNode.getSize());
                                dataNode.setCollectionFlag(false);
                                dataNode.setLastModified(contentNode.getLastModified());
                                dataNode.setNodeInfoURL(
                                        UriBuilder.fromUri(endpointBaseURI)
                                                .path(Constants.AGENTSTORAGE_URI_PATH)
                                                .path("storage_volume")
                                                .path(storageVolume.getId().toString())
                                                .path("data_info")
                                                .path(dataNode.getNodeRelativePath())
                                                .build()
                                                .toString());
                                dataNode.setNodeAccessURL(
                                        UriBuilder.fromUri(endpointBaseURI)
                                                .path(Constants.AGENTSTORAGE_URI_PATH)
                                                .path("storage_volume")
                                                .path(storageVolume.getId().toString())
                                                .path("data_content")
                                                .path(dataNode.getNodeRelativePath())
                                                .build()
                                                .toString());
                                return dataNode;
                            })
                            .collect(Collectors.toList());
                    return Response.ok(dataNodes, MediaType.APPLICATION_JSON);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND))
                .build();
    }

}
