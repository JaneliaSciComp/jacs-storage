package org.janelia.jacsstorage.rest;

import java.net.URI;

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
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
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

    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Inject @RemoteInstance
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
    public Response redirectForContentCheck(@PathParam("filePath") String filePathParam,
                                            @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                                            @Context UriInfo requestURI) {
        LOG.info("Check {}", filePathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(null, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(filePathParam),
                (dataBundle, dataEntryPath) -> dataBundle.getStorageVolume()
                        .map(storageVolume -> {
                            URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                                    .path("agent_storage")
                                    .path(dataBundle.getId().toString())
                                    .path("data_content")
                                    .path(dataEntryPath)
                                    .replaceQuery(requestURI.getRequestUri().getRawQuery())
                                    .build();
                            LOG.info("Redirect to {} for checking {}", redirectURI, filePathParam);
                            return Response.temporaryRedirect(redirectURI);
                        })
                        .orElseGet(() -> Response.status(Response.Status.BAD_REQUEST)
                                .header("Content-Length", 0)),
                (storageVolume, storageDataPathURI) -> {
                    if (StringUtils.isNotBlank(storageVolume.getStorageServiceURL())) {
                        URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                                .path("agent_storage")
                                .path("storage_volume")
                                .path(storageVolume.getId().toString())
                                .path("data_content")
                                .path(storageVolume.getPathRelativeToBaseStorageRoot(storageDataPathURI.getStoragePath()).toString())
                                .replaceQuery(requestURI.getRequestUri().getRawQuery())
                                .build();
                        LOG.info("Redirect to {} for checking {}", redirectURI, filePathParam);
                        return Response.temporaryRedirect(redirectURI);
                    } else {
                        return Response.status(Response.Status.BAD_GATEWAY)
                                .header("Content-Length", 0);
                    }
                }
        ).build();
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
    public Response redirectForContent(@PathParam("filePath") String filePathParam, @Context UriInfo requestURI) {
        LOG.info("Redirecting to agent for getting content of {}", filePathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(null, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(filePathParam),
                (dataBundle, dataEntryPath) -> dataBundle.getStorageVolume()
                            .map(storageVolume -> {
                                URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                                        .path("agent_storage")
                                        .path(dataBundle.getId().toString())
                                        .path("data_content")
                                        .path(dataEntryPath)
                                        .replaceQuery(requestURI.getRequestUri().getRawQuery())
                                        .build();
                                LOG.info("Redirect to {} for getting content from {}", redirectURI, filePathParam);
                                return Response.temporaryRedirect(redirectURI);
                            })
                            .orElseGet(() -> Response
                                    .status(Response.Status.BAD_REQUEST.getStatusCode())
                                    .entity(new ErrorResponse("No volume associated with databundle " + dataBundle.getId()))
                                    .type(MediaType.APPLICATION_JSON)
                            ),
                (storageVolume, storageDataPathURI) -> {
                    if (StringUtils.isNotBlank(storageVolume.getStorageServiceURL())) {
                        URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                                .path("agent_storage")
                                .path("storage_volume")
                                .path(storageVolume.getId().toString())
                                .path("data_content")
                                .path(storageVolume.getPathRelativeToBaseStorageRoot(storageDataPathURI.getStoragePath()).toString())
                                .replaceQuery(requestURI.getRequestUri().getRawQuery())
                                .build();
                        LOG.info("Redirect to {} for getting content from {}", redirectURI, filePathParam);
                        return Response.temporaryRedirect(redirectURI);
                    } else {
                        return Response.status(Response.Status.BAD_GATEWAY)
                                .entity(new ErrorResponse("No storage service URL found to serve " + storageDataPathURI))
                                .type(MediaType.APPLICATION_JSON)
                                ;
                    }
                }
        ).build();
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
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(null, storageLookupService, storageVolumeManager);
        StoragePathURI storagePathURI = StoragePathURI.createAbsolutePathURI(filePathParam);
        return storageResourceHelper.getStorageVolumesForURI(storagePathURI).stream()
                .map(storageVolume -> {
                    URI redirectURI = UriBuilder.fromUri(URI.create(storageVolume.getStorageServiceURL()))
                            .path("agent_storage")
                            .path("storage_path/data_content")
                            .path(storagePathURI.toString())
                            .replaceQuery(requestURI.getRequestUri().getRawQuery())
                            .build();
                    LOG.info("Redirect to {} to delete {}", redirectURI, filePathParam);
                    return Response.temporaryRedirect(redirectURI).build();
                })
                .findFirst()
                .orElseGet(() -> Response
                        .status(Response.Status.BAD_GATEWAY)
                        .entity(new ErrorResponse("No storage service URL found to serve " + filePathParam))
                        .build())
                ;
    }

}
