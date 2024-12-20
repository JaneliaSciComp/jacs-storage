package org.janelia.jacsstorage.rest;

import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.requesthelpers.ContentAccessRequestHelper;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name="AgentPathAccess", description = "Agent storage API based on file's path.")
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

    @Operation(description = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The content was found"),
            @ApiResponse(responseCode = "404", description = "Invalid file path"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @HEAD
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response checkPath(@PathParam("dataPath") String dataPathParam,
                              @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                              @Context ContainerRequestContext requestContext) {
        try {
            LOG.debug("Start check path {}", dataPathParam);
            StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
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
                    .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI))
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

    @Operation(description = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle identifier"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response retrieveContent(@PathParam("dataPath") String dataPathParam,
                                    @Context UriInfo requestURI,
                                    @Context ContainerRequestContext requestContext) {
        try {
            LOG.debug("Start retrieve data from {}", dataPathParam);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
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
                    .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI))
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

    @Operation(description = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The data streaming was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle identifier"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @LogStorageEvent(
            eventName = "DELETE_STORAGE_ENTRY",
            argList = {0, 1}
    )
    @RequireAuthentication
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response removeData(@PathParam("dataPath") String dataPathParam,
                               @Context ContainerRequestContext requestContext,
                               @Context SecurityContext securityContext) {
        try {
            LOG.debug("Remove data from {}", dataPathParam);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
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
                    .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI))
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

    @Operation(description = "Store the content at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "The data was saved successfully"),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle identifier"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1}
    )
    @RequireAuthentication
    @PUT
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path/data_content/{dataPath:.+}")
    public Response storeData(@PathParam("dataPath") String dataPathParam,
                              @Context ContainerRequestContext requestContext,
                              @Context SecurityContext securityContext,
                              InputStream contentStream) {
        LOG.debug("Retrieve data from {}", dataPathParam);
        JADEOptions storageOptions = JADEOptions.create()
                .setAccessKey(requestContext.getHeaderString("AccessKey"))
                .setSecretKey(requestContext.getHeaderString("SecretKey"))
                .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
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
                .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI)
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
                    newContentNode.setStorageType(
                        storageVolume.getStorageType() != null ? storageVolume.getStorageType().name() : null
                    );
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

    @Operation(description = "Inspect and retrieve content info of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The operation was successfull. " +
                    "As a note the operation is considered successful even if there's currently no process to extract any information from the content"),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle identifier"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @RequireAuthentication
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path/data_info/{dataPath:.+}")
    public Response retrieveContentMetadata(@PathParam("dataPath") String dataPathParam,
                                            @Context ContainerRequestContext requestContext,
                                            @Context UriInfo requestURI) {
        try {
            LOG.debug("Retrieve metadata from {}", dataPathParam);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
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
                    .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI))
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

    @Operation(
            description = "List the content."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully read the data bundle content."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @RequireAuthentication
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_path/list/{dataPath:.*}")
    public Response listContent(@PathParam("dataPath") String dataPathParam,
                                @QueryParam("depth") Integer depthParam,
                                @QueryParam("offset") Integer offsetParam,
                                @QueryParam("length") Integer lengthParam,
                                @QueryParam("directoriesOnly") Boolean directoriesOnlyParam,
                                @Context UriInfo requestURI,
                                @Context ContainerRequestContext requestContext,
                                @Context SecurityContext securityContext) {
        LOG.debug("List content from location {} with a depthParameter {}", dataPathParam, depthParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEOptions storageOptions = JADEOptions.create()
                .setAccessKey(requestContext.getHeaderString("AccessKey"))
                .setSecretKey(requestContext.getHeaderString("SecretKey"))
                .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        int offset = offsetParam != null ? offsetParam : 0;
        int length = lengthParam != null ? lengthParam : -1;
        boolean directoriesOnly = directoriesOnlyParam != null ? directoriesOnlyParam : false;
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
                .setStartEntryIndex(offset)
                .setDirectoriesOnly(directoriesOnly);
        return accessibleVolumes.stream()
                .findFirst()
                .flatMap(aStorageVolume -> aStorageVolume.setStorageOptions(storageOptions).resolveAbsoluteLocationURI(contentURI)
                        .map(resolvedContentURI -> Pair.of(aStorageVolume, resolvedContentURI)))
                .map(volAndContentURIPair -> {
                    JacsStorageVolume storageVolume = volAndContentURIPair.getLeft();
                    JADEStorageURI resolvedContentURI = volAndContentURIPair.getRight();
                    JADEStorageURI storageVolumeURI = storageVolume.getVolumeStorageRootURI();
                    ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                    List<DataNodeInfo> dataNodes = contentGetter.getObjectsList().stream()
                            .map(contentNode -> {
                                DataNodeInfo dataNode = new DataNodeInfo();
                                dataNode.setStorageType(contentNode.getStorageType().name());
                                dataNode.setStorageRootLocation(storageVolume.getStorageRootLocation());
                                dataNode.setStorageRootBinding(storageVolumeURI.getJadeStorage());
                                dataNode.setNodeRelativePath(storageVolumeURI.relativizeKey(contentNode.getObjectKey()));
                                dataNode.setMimeType(contentNode.getMimeType());
                                dataNode.setSize(contentNode.getSize());
                                dataNode.setCollectionFlag(contentNode.isCollection());
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
