package org.janelia.jacsstorage.rest;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
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
import jakarta.ws.rs.core.UriInfo;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "StorageVolumes", description = "Storage Volumes API.")
@Timed
@RequireAuthentication
@Path("storage_volumes")
public class StorageVolumesResource {
    private static final Logger LOG = LoggerFactory.getLogger(StorageVolumesResource.class);

    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;
    @Context
    private UriInfo resourceURI;

    @Operation(
            description = "Retrieve storage volume by ID."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "The storage entry with the given ID"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "If user is not authenticated"
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "If entry ID is invalid"
            ),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
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
    public Response listStorageVolumes(@QueryParam("id") Long storageVolumeId,
                                       @QueryParam("name") String volumeName,
                                       @QueryParam("shared") boolean shared,
                                       @QueryParam("storageAgent") String storageAgent,
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
        JADEStorageURI dataStorageURI = JADEStorageURI.createStoragePathURI(dataStoragePathParam, storageOptions);
        StorageQuery storageQuery = new StorageQuery()
                .setId(storageVolumeId)
                .setStorageName(volumeName)
                .setStorageType(dataStorageURI.getStorageType())
                .setShared(shared)
                .setAccessibleOnAgent(storageAgent)
                .setStorageTags(storageTags)
                .setStorageVirtualPath(storageVirtualPath)
                .setDataStoragePath(dataStorageURI.getJadeStorage())
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

    @Operation(
            description = "Update an existing or create a new storage volume."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Updated storage entry."),
            @ApiResponse(responseCode = "201", description = "Created new storage entry."),
            @ApiResponse(responseCode = "401", description = "If user is not authenticated"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "UPDATE_OR_CREATE_STORAGE_VOLUME",
            argList = {0, 1}
    )
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response postUpdateStorageVolume(@Parameter(description = "information about the volume to be created") JacsStorageVolume jacsStorageVolume,
                                            @Context SecurityContext securityContext) {
        LOG.info("Create storage: {} with credentials {}", jacsStorageVolume, securityContext.getUserPrincipal());
        if (jacsStorageVolume.hasId()) {
            return updateStorageVolume(jacsStorageVolume.getId(), jacsStorageVolume);
        } else {
            JacsStorageVolume newStorageVolume = storageVolumeManager.createNewStorageVolume(jacsStorageVolume);
            return Response
                    .created(resourceURI.getBaseUriBuilder().path(newStorageVolume.getId().toString()).build())
                    .entity(newStorageVolume)
                    .build();
        }
    }

    @Operation(
            description = "Update an existing or create a new storage volume."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Updated storage entry."),
            @ApiResponse(responseCode = "201", description = "Created new storage entry."),
            @ApiResponse(responseCode = "401", description = "If user is not authenticated"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "UPDATE_OR_CREATE_STORAGE_VOLUME",
            argList = {0, 1}
    )
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Response putUpdateStorageVolume(@PathParam("id") Long storageVolumeId,
                                           @Parameter(description = "information about the volume to be created") JacsStorageVolume jacsStorageVolume,
                                           @Context SecurityContext securityContext) {
        LOG.info("Update storage: {} with credentials {}", jacsStorageVolume, securityContext.getUserPrincipal());
        return updateStorageVolume(storageVolumeId, jacsStorageVolume);
    }

    private Response updateStorageVolume(Number jacsStorageVolumeId, JacsStorageVolume jacsStorageVolume) {
        long currentTime = System.currentTimeMillis();
        JacsStorageVolume updatedStorageVolume = storageVolumeManager.updateVolumeInfo(jacsStorageVolumeId, jacsStorageVolume);
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
