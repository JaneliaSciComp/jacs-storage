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
import org.janelia.jacsstorage.agent.AgentState;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
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

@Tag(name = "VolumeStorage", description = "Agent storage API on a particular volume")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class VolumeStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeStorageResource.class);

    @Inject
    private DataContentService dataContentService;
    @Inject
    @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject
    private AgentState agentState;

    @Operation(description = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The content was found"),
            @ApiResponse(responseCode = "404", description = "Invalid file path"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @HEAD
    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response checkDataPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                   @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                   @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                                                   @Context ContainerRequestContext requestContext) {
        try {
            LOG.debug("Check data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
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
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        boolean contentFound = dataContentService.exists(resolvedContentURI);
                        if (contentFound) {
                            return Response.ok();
                        } else {
                            return Response.status(Response.Status.NOT_FOUND);
                        }
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
                    .build();
        } finally {
            LOG.debug("Complete check data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        }
    }

    @Operation(description = "Stream the specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or invalid file path"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response retrieveDataContentFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                         @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                         @Context ContainerRequestContext requestContext,
                                                         @Context UriInfo requestURI) {
        try {
            LOG.debug("Retrieve data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
            if (storageVolume == null) {
                LOG.warn("No accessible volume found for {}", storageVolumeId);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
                LOG.warn("Attempt to read {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
                return Response
                        .status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        long startTime = System.currentTimeMillis();
                        ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                        long contentSize = contentGetter.estimateContentSize();
                        LOG.debug("Retrieved {} size ({}) in {} secs", resolvedContentURI, contentSize, (System.currentTimeMillis()-startTime)/1000.);
                        StreamingOutput outputStream = output -> {
                            try {
                                contentGetter.streamContent(output);
                                LOG.debug("Finished streaming {} in {} secs", resolvedContentURI, (System.currentTimeMillis()-startTime)/1000.);
                            } finally {
                                output.flush();
                            }
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
            LOG.debug("Complete retrieving data from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        }
    }

    @Operation(description = "Stream content info of specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The operation  was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or invalid file path"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_info/{storageRelativePath:.+}")
    public Response retrieveMetadataFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                      @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                      @Context ContainerRequestContext requestContext,
                                                      @Context UriInfo requestURI) {
        try {
            LOG.debug("Retrieve metadata from volume {}:{}", storageVolumeId, storageRelativeFilePath);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
            if (storageVolume == null) {
                LOG.warn("No accessible volume found for {}", storageVolumeId);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                        .build();
            } else if (!storageVolume.hasPermission(JacsStoragePermission.READ)) {
                LOG.warn("Attempt to read {} metadata from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
                return Response
                        .status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                        return Response.ok(contentGetter.getMetaData());
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
                    .build();
        } finally {
            LOG.debug("Complete retrieving metadata from volume {}:{}", storageVolumeId, storageRelativeFilePath);
        }
    }

    @Operation(description = "List the content of the specified path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The listing was successful"),
            @ApiResponse(responseCode = "404", description = "Invalid file path"),
            @ApiResponse(responseCode = "409", description = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_volume/{storageVolumeId}/list/{storageRelativePath:.*}")
    public Response listPathFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                              @PathParam("storageRelativePath") String storageRelativeFilePath,
                                              @QueryParam("depth") Integer depthParam,
                                              @QueryParam("offset") Integer offsetParam,
                                              @QueryParam("length") Integer lengthParam,
                                              @QueryParam("directoriesOnly") Boolean directoriesOnlyParam,
                                              @Context ContainerRequestContext requestContext,
                                              @Context UriInfo requestURI) {
        try {
            LOG.debug("List data from volume {}:{} with a depthParameter {}", storageVolumeId, storageRelativeFilePath, depthParam);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
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
            int offset = offsetParam != null ? offsetParam : 0;
            int length = lengthParam != null ? lengthParam : -1;
            boolean directoriesOnly = directoriesOnlyParam != null ? directoriesOnlyParam : false;
            URI endpointBaseURI = requestURI.getBaseUri();
            ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters())
                    .setMaxDepth(depth)
                    .setEntriesCount(length)
                    .setStartEntryIndex(offset)
                    .setDirectoriesOnly(directoriesOnly);
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                        List<DataNodeInfo> dataNodes = contentGetter.getObjectsList().stream()
                                .map(contentNode -> {
                                    DataNodeInfo dataNode = new DataNodeInfo();
                                    dataNode.setStorageType(contentNode.getStorageType().name());
                                    dataNode.setStorageRootLocation(storageVolume.getStorageRootLocation());
                                    dataNode.setStorageRootBinding(storageVolume.getStorageVirtualPath());
                                    dataNode.setNodeRelativePath(storageVolume.getContentRelativePath(contentNode.getNodeStorageURI()));
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
        } finally {
            LOG.debug("Complete list data from volume {}:{} with a depthParameter {}", storageVolumeId, storageRelativeFilePath, depthParam);
        }
    }

    @Operation(description = "Store the content on the specified volume at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "The data was saved successfully"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier"),
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
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response storeDataContentOnStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                    @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                    @Context ContainerRequestContext requestContext,
                                                    @Context SecurityContext securityContext,
                                                    @Context UriInfo requestURI,
                                                    InputStream contentStream) {
        try {
            LOG.debug("Store data to {}: {}", storageVolumeId, storageRelativeFilePath);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
            if (storageVolume == null) {
                LOG.warn("No accessible volume found for {}", storageVolumeId);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            } else if (!storageVolume.hasPermission(JacsStoragePermission.WRITE)) {
                LOG.warn("Attempt to write {} on volume {} but the volume does not allow WRITE", storageRelativeFilePath, storageVolumeId);
                return Response
                        .status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No write permission for volume " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        long size = dataContentService.writeDataStream(resolvedContentURI, contentStream);
                        URI newContentURI = UriBuilder.fromUri(requestURI.getBaseUri())
                                .path(Constants.AGENTSTORAGE_URI_PATH)
                                .path("storage_path/data_content")
                                .path(resolvedContentURI.getJadeStorage())
                                .build();
                        DataNodeInfo newContentNode = new DataNodeInfo();
                        newContentNode.setStorageType(
                                storageVolume.getStorageType() != null ? storageVolume.getStorageType().name() : null
                        );
                        newContentNode.setStorageRootBinding(storageVolume.getStorageVirtualPath());
                        newContentNode.setStorageRootLocation(storageVolume.getStorageRootLocation());
                        newContentNode.setNodeInfoURL(UriBuilder.fromUri(requestURI.getBaseUri())
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
        } finally {
            LOG.debug("Complete store data to {}: {}", storageVolumeId, storageRelativeFilePath);
        }
    }

    @Operation(description = "Delete specified data file identified by the relative path to the volume mount point.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "The delete was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or invalid file path"),
            @ApiResponse(responseCode = "500", description = "Data delete error")
    })
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_volume/{storageVolumeId}/data_content/{storageRelativePath:.+}")
    public Response deleteDataContentFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                       @PathParam("storageRelativePath") String storageRelativeFilePath,
                                                       @Context ContainerRequestContext requestContext) {
        try {
            LOG.debug("Delete data {}:{}", storageVolumeId, storageRelativeFilePath);
            JADEOptions storageOptions = JADEOptions.create()
                    .setAccessKey(requestContext.getHeaderString("AccessKey"))
                    .setSecretKey(requestContext.getHeaderString("SecretKey"))
                    .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
            JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
            if (storageVolume == null) {
                LOG.warn("No accessible volume found for {}", storageVolumeId);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No accessible volume found for " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            } else if (!storageVolume.hasPermission(JacsStoragePermission.DELETE)) {
                LOG.warn("Attempt to read {} from volume {} but the volume does not allow READ", storageRelativeFilePath, storageVolumeId);
                return Response
                        .status(Response.Status.FORBIDDEN)
                        .entity(new ErrorResponse("No read permission for volume " + storageVolumeId))
                        .type(MediaType.APPLICATION_JSON)
                        .build();
            }
            return storageVolume
                    .setStorageOptions(storageOptions)
                    .resolveRelativeLocation(storageRelativeFilePath)
                    .map(resolvedContentURI -> {
                        dataContentService.removeData(resolvedContentURI);
                        return Response.noContent();
                    })
                    .orElse(Response.status(Response.Status.NOT_FOUND))
                    .build();
        } finally {
            LOG.debug("Complete delete data {}:{}", storageVolumeId, storageRelativeFilePath);
        }
    }

    @Operation(
            description = "List storage volumes. The volumes could be filtered by {id, storageHost, storageTags, volumeName}."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "The list of storage entries that match the given filters"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "If user is not authenticated"
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Data read error"
            )
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("storage_volumes")
    public Response listStorageVolumes(@QueryParam("id") Long storageVolumeId,
                                       @QueryParam("name") String volumeName,
                                       @QueryParam("storageType") String storageType,
                                       @QueryParam("shared") boolean shared,
                                       @QueryParam("storageTags") List<String> storageTags,
                                       @QueryParam("storageVirtualPath") String storageVirtualPath,
                                       @QueryParam("dataStoragePath") String dataStoragePathParam,
                                       @QueryParam("includeInactive") boolean includeInactive,
                                       @QueryParam("includeInaccessibleVolumes") boolean includeInaccessibleVolumes,
                                       @Context ContainerRequestContext requestContext,
                                       @Context SecurityContext securityContext) {
        JADEOptions storageOptions = JADEOptions.create()
                .setAccessKey(requestContext.getHeaderString("AccessKey"))
                .setSecretKey(requestContext.getHeaderString("SecretKey"))
                .setAWSRegion(requestContext.getHeaderString("AWSRegion"));
        JADEStorageURI jadeStorageURI = JADEStorageURI.createStoragePathURI(dataStoragePathParam, storageOptions);
        StorageQuery storageQuery = new StorageQuery()
                .setStorageType(JacsStorageType.fromName(storageType))
                .setId(storageVolumeId)
                .setStorageName(volumeName)
                .setShared(shared)
                .setAccessibleOnAgent(agentState.getLocalAgentId())
                .setStorageTags(storageTags)
                .setStorageVirtualPath(storageVirtualPath)
                .setDataStoragePath(jadeStorageURI.getJadeStorage())
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
