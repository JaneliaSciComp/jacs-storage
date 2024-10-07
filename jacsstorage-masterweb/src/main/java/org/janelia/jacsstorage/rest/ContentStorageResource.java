package org.janelia.jacsstorage.rest;

import java.net.URI;
import java.util.List;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Api(value = "File path based API for retrieving storage content")
@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path("storage_content")
public class ContentStorageResource {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageResource.class);

    @Inject
    @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    /**
     * Redirect to the agent URL for checking the content using the file path.
     *
     * @param filePathParam
     * @param directoryOnlyParam
     * @return
     */
    @ApiOperation(value = "Get file content",
            notes = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 502, message = "Bad ", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    @HEAD
    @Path("storage_path_redirect/{filePath:.+}")
    public Response redirectForContentCheckFromPathParam(@PathParam("filePath") String filePathParam,
                                                         @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                                                         @Context UriInfo requestURI) {
        LOG.info("Check {}", filePathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        if (StringUtils.isNotBlank(filePathParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(filePathParam);
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid file path"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return volumeCandidates.stream()
                .findFirst()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(contentURI.getJadeStorage())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} for checking {}", redirectURI, filePathParam);
                    return Response.temporaryRedirect(redirectURI);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for checking the content using the file path.
     *
     * @param filePathQueryParam
     * @param directoryOnlyParam
     * @return
     */
    @ApiOperation(value = "Get file content",
            notes = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 502, message = "Bad ", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    @HEAD
    @Path("query_storage_path")
    public Response redirectForContentCheckFromQueryParam(@QueryParam("storagePath") String filePathQueryParam,
                                                          @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                                                          @Context UriInfo requestURI) {
        LOG.info("Check {}", filePathQueryParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        if (StringUtils.isNotBlank(filePathQueryParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(filePathQueryParam);
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid file path"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return volumeCandidates.stream()
                .findFirst()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(contentURI.getJadeStorage())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} for checking {}", redirectURI, filePathQueryParam);
                    return Response.temporaryRedirect(redirectURI);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for retrieving the content using the file path.
     *
     * @param filePathParam
     * @param requestURI
     * @return
     */
    @ApiOperation(value = "Get file content",
            notes = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 502, message = "Bad ", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path_redirect/{filePath:.+}")
    public Response redirectForContentFromPathParam(@PathParam("filePath") String filePathParam,
                                                    @Context UriInfo requestURI) {
        LOG.info("Redirecting to agent for getting content of {}", filePathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        if (StringUtils.isNotBlank(filePathParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(filePathParam);
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid file path"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return volumeCandidates.stream()
                .findFirst()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(contentURI.getJadeStorage())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} for getting content from {}", redirectURI, filePathParam);
                    return Response.temporaryRedirect(redirectURI);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for retrieving the content using the file path.
     *
     * @param filePathQueryParam
     * @param requestURI
     * @return
     */
    @ApiOperation(value = "Get file content",
            notes = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 502, message = "Bad ", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path_redirect")
    public Response redirectForContentFromQueryParam(@QueryParam("storagePath") String filePathQueryParam,
                                                     @Context UriInfo requestURI) {
        LOG.info("Redirecting to agent for getting content of {}", filePathQueryParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        if (StringUtils.isNotBlank(filePathQueryParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(filePathQueryParam);
        } else {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Invalid file path"))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return volumeCandidates.stream()
                .findFirst()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(contentURI.getJadeStorage())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} for getting content from {}", redirectURI, filePathQueryParam);
                    return Response.temporaryRedirect(redirectURI);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for deleting the content using the file path.
     *
     * @param filePathParam
     * @return an HTTP Redirect response if a storage volume is found for the given path or BAD_GATEWAY otherwise
     */
    @ApiOperation(value = "Delete file content",
            notes = "Return the redirect URL to for deleting the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(code = 307, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 502, message = "Bad ", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    @RequireAuthentication
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path_redirect/{filePath:.+}")
    public Response redirectForDeleteContent(@PathParam("filePath") String filePathParam, @Context UriInfo requestURI) {
        LOG.info("Redirect to agent for deleting content of {}", filePathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(filePathParam);
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI);
        } catch (IllegalArgumentException e) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse(e.getMessage()))
                    .type(MediaType.APPLICATION_JSON)
                    .build();
        }
        return volumeCandidates.stream()
                .findFirst()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(contentURI.getJadeStorage())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} for getting content from {}", redirectURI, filePathParam);
                    return Response.temporaryRedirect(redirectURI);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

}
