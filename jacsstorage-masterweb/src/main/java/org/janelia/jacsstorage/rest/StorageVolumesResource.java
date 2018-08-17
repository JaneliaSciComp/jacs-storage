package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
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
import java.util.List;

@Timed
@RequireAuthentication
@Produces(MediaType.APPLICATION_JSON)
@Path("storage_volumes")
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "jwtBearerToken", name = "Authorization", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(value = "Storage Volumes API.")
public class StorageVolumesResource {
    private static final Logger LOG = LoggerFactory.getLogger(StorageVolumesResource.class);

    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;
    @Context
    private UriInfo resourceURI;

    @GET
    @Path("{id}")
    @ApiOperation(
            value = "Retrieve storage volume by ID.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The storage entry with the given ID",
                    response = JacsStorageVolume.class
            ),
            @ApiResponse(
                    code = 401,
                    message = "If user is not authenticated",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 404,
                    message = "If entry ID is invalid",
                    response = ErrorResponse.class
            ),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getStorageVolumeById(@PathParam("id") Long storageVolumeId, @Context SecurityContext securityContext) {
        LOG.info("Retrieve storage volume info for {}", storageVolumeId);
        JacsStorageVolume jacsStorageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (jacsStorageVolume == null) {
            LOG.warn("No storage found for {}", storageVolumeId);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No storage volume found for " + storageVolumeId))
                    .build();
        } else {
            return Response
                    .ok(jacsStorageVolume)
                    .build();
        }
    }

    @GET
    @ApiOperation(
            value = "List storage volumes. The volumes could be filtered by {id, storageHost, storageTags, volumeName}.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The list of storage entries that match the given filters",
                    response = DataStorageInfo.class
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
    public Response listStorageVolumes(@QueryParam("id") Long storageVolumeId,
                                       @QueryParam("name") String volumeName,
                                       @QueryParam("shared") boolean shared,
                                       @QueryParam("storageHost") String storageHost,
                                       @QueryParam("storageTags") List<String> storageTags,
                                       @QueryParam("storageVirtualPath") String storagePathPrefix,
                                       @Context SecurityContext securityContext) {
        StorageQuery storageQuery = new StorageQuery()
                .setId(storageVolumeId)
                .setStorageName(volumeName)
                .setShared(shared)
                .setAccessibleOnHost(storageHost)
                .setStorageTags(storageTags)
                .setStorageVirtualPath(storagePathPrefix);
        LOG.info("List storage volumes filtered with: {}", storageQuery);
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        PageResult<JacsStorageVolume> results = new PageResult<>();
        results.setPageSize(storageVolumes.size());
        results.setResultList(storageVolumes);
        return Response
                .ok(results)
                .build();
    }

    @LogStorageEvent(
            eventName = "UPDATE_OR_CREATE_STORAGE_VOLUME",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Consumes("application/json")
    @POST
    @ApiOperation(
            value = "Update an existing or create a new storage volume.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Updated storage entry.", response = JacsStorageVolume.class),
            @ApiResponse(code = 201, message = "Created new storage entry.", response = JacsStorageVolume.class),
            @ApiResponse(code = 401, message = "If user is not authenticated", response = ErrorResponse.class),
            @ApiResponse(code = 500, message = "Data write error", response = ErrorResponse.class)
    })
    public Response postUpdateStorageVolume(@ApiParam(value = "information about the volume to be created") JacsStorageVolume jacsStorageVolume,
                                        @Context SecurityContext securityContext) {
        LOG.info("Create storage: {} with credentials {}", jacsStorageVolume, securityContext.getUserPrincipal());
        return updateStorageVolume(jacsStorageVolume);
    }

    @LogStorageEvent(
            eventName = "UPDATE_OR_CREATE_STORAGE_VOLUME",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Consumes("application/json")
    @PUT
    @ApiOperation(
            value = "Update an existing or create a new storage volume.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Updated storage entry.", response = JacsStorageVolume.class),
            @ApiResponse(code = 201, message = "Created new storage entry.", response = JacsStorageVolume.class),
            @ApiResponse(code = 401, message = "If user is not authenticated", response = ErrorResponse.class),
            @ApiResponse(code = 500, message = "Data write error", response = ErrorResponse.class)
    })
    public Response putUpdateStorageVolume(@ApiParam(value = "information about the volume to be created") JacsStorageVolume jacsStorageVolume,
                                           @Context SecurityContext securityContext) {
        LOG.info("Update storage: {} with credentials {}", jacsStorageVolume, securityContext.getUserPrincipal());
        return updateStorageVolume(jacsStorageVolume);
    }

    private Response updateStorageVolume(JacsStorageVolume jacsStorageVolume) {
        long currentTime = System.currentTimeMillis();
        JacsStorageVolume updatedStorageVolume = storageVolumeManager.updateVolumeInfo(jacsStorageVolume);
        long volumeCreatedTimestamp = updatedStorageVolume.getCreated().getTime();
        if (volumeCreatedTimestamp - currentTime > 0) {
            return Response
                    .created(resourceURI.getBaseUriBuilder().path(updatedStorageVolume.getId().toString()).build())
                    .entity(updatedStorageVolume)
                    .build();
        } else {
            return Response
                    .ok(updatedStorageVolume)
                    .build();
        }
    }

}
