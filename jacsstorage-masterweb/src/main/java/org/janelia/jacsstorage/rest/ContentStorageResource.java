package org.janelia.jacsstorage.rest;

import java.net.URI;
import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "MasterContent", description = "File path based API for retrieving storage content")
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
     * @param contentPath
     * @return
     */
    @Operation(summary = "Get file content",
            description = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @HEAD
    @Path("storage_path_redirect")
    public Response redirectForCheckContentWithQueryParam(@QueryParam("contentPath") String contentPath,
                                                          @Context ContainerRequestContext requestContext,
                                                          @Context UriInfo requestURI) {
        return processHeadToCheckContent(contentPath, requestContext, requestURI);
    }

    /**
     * Redirect to the agent URL for checking the content using the file path.
     *
     * @param contentPath
     * @return
     */
    @Operation(summary = "Get file content",
            description = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @HEAD
    @Path("storage_path_redirect/{contentPath:.+}")
    public Response redirectForCheckContentWithPathParam(@PathParam("contentPath") String contentPath,
                                                         @Context ContainerRequestContext requestContext,
                                                         @Context UriInfo requestURI) {
        return processHeadToCheckContent(contentPath, requestContext, requestURI);
    }

    private Response processHeadToCheckContent(String contentPathParam, ContainerRequestContext requestContext, UriInfo requestURI) {
        LOG.info("Check {}", contentPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        String accessKey = requestContext.getHeaderString("AccessKey");
        String secretKey = requestContext.getHeaderString("SecretKey");
        String awsRegion = requestContext.getHeaderString("AWSRegion");
        if (StringUtils.isNotBlank(contentPathParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(
                    contentPathParam,
                    JADEOptions.create()
                            .setAccessKey(accessKey)
                            .setSecretKey(secretKey)
                            .setAWSRegion(awsRegion)
            );
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
                    LOG.info("Redirect to {} for checking {}", redirectURI, contentPathParam);
                    return Response.temporaryRedirect(redirectURI)
                            .header("AccessKey", accessKey)
                            .header("SecretKey", secretKey);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for retrieving the content using the file path.
     *
     * @param contentPath
     * @param requestURI
     * @return
     */
    @Operation(summary = "Get file content",
            description = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path_redirect")
    public Response redirectForGetContentWithQueryParam(@QueryParam("contentPath") String contentPath,
                                                        @Context ContainerRequestContext requestContext,
                                                        @Context UriInfo requestURI) {
        return processGetToRetrieveContent(contentPath, requestContext, requestURI);
    }

    /**
     * Redirect to the agent URL for retrieving the content using the file path.
     *
     * @param contentPath
     * @param requestContext
     * @param requestURI
     * @return
     */
    @Operation(summary = "Get file content",
            description = "Return the redirect URL to for retrieving the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("storage_path_redirect/{contentPath:.+}")
    public Response redirectForGetContentWithPathParam(@PathParam("contentPath") String contentPath,
                                                       @Context ContainerRequestContext requestContext,
                                                       @Context UriInfo requestURI) {
        return processGetToRetrieveContent(contentPath, requestContext, requestURI);
    }

    private Response processGetToRetrieveContent(String contentPathParam,
                                                 ContainerRequestContext requestContext,
                                                 UriInfo requestURI) {
        LOG.info("Redirecting to agent for getting content of {}", contentPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        JADEStorageURI contentURI;
        String accessKey = requestContext.getHeaderString("AccessKey");
        String secretKey = requestContext.getHeaderString("SecretKey");
        String awsRegion = requestContext.getHeaderString("AWSRegion");
        if (StringUtils.isNotBlank(contentPathParam)) {
            contentURI = JADEStorageURI.createStoragePathURI(
                    contentPathParam,
                    JADEOptions.create()
                            .setAccessKey(accessKey)
                            .setSecretKey(secretKey)
                            .setAWSRegion(awsRegion)
            );
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
                    LOG.info("Redirect to {} for getting content from {}", redirectURI, contentPathParam);
                    return Response.temporaryRedirect(redirectURI)
                            .header("AccessKey", accessKey)
                            .header("SecretKey", secretKey);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

    /**
     * Redirect to the agent URL for deleting the content using the file path.
     *
     * @param contentPath
     * @return an HTTP Redirect response if a storage volume is found for the given path or BAD_GATEWAY otherwise
     */
    @Operation(summary = "Delete file content",
            description = "Return the redirect URL to for deleting the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @RequireAuthentication
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path_redirect/{contentPath:.+}")
    public Response redirectForDeleteContentWithPathParam(@PathParam("contentPath") String contentPath,
                                                          @Context ContainerRequestContext requestContext,
                                                          @Context UriInfo requestURI) {
        return processDeleteToRemoveContent(contentPath, requestContext, requestURI);
    }

    /**
     * Redirect to the agent URL for deleting the content using the file path.
     *
     * @param contentPath
     * @return an HTTP Redirect response if a storage volume is found for the given path or BAD_GATEWAY otherwise
     */
    @Operation(summary = "Delete file content",
            description = "Return the redirect URL to for deleting the content based on the file path")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "307", description = "Success"),
            @ApiResponse(responseCode = "502", description = "Bad "),
            @ApiResponse(responseCode = "404", description = "Specified file path not found")
    })
    @RequireAuthentication
    @DELETE
    @Produces({MediaType.APPLICATION_JSON})
    @Path("storage_path_redirect")
    public Response redirectForDeleteContentWithQueryParam(@QueryParam("contentPath") String contentPath,
                                                           @Context ContainerRequestContext requestContext,
                                                           @Context UriInfo requestURI) {
        return processDeleteToRemoveContent(contentPath, requestContext, requestURI);
    }

    private Response processDeleteToRemoveContent(String contentPathParam,
                                                  ContainerRequestContext requestContext,
                                                  UriInfo requestURI) {
        LOG.info("Redirect to agent for deleting content of {}", contentPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
        String accessKey = requestContext.getHeaderString("AccessKey");
        String secretKey = requestContext.getHeaderString("SecretKey");
        String awsRegion = requestContext.getHeaderString("AWSRegion");
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(
                contentPathParam,
                JADEOptions.create()
                        .setAccessKey(accessKey)
                        .setSecretKey(secretKey)
                        .setAWSRegion(awsRegion)
        );
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
                    LOG.info("Redirect to {} for getting content from {}", redirectURI, contentPathParam);
                    return Response.temporaryRedirect(redirectURI)
                            .header("AccessKey", accessKey)
                            .header("SecretKey", secretKey);
                })
                .orElse(Response.status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No managed volume found for " + contentURI))
                        .type(MediaType.APPLICATION_JSON))
                .build();
    }

}
