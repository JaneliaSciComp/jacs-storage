package org.janelia.jacsstorage.rest;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
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
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "MasterStorage", description = "Master storage API.")
@Timed
@Path("storage")
public class MasterStorageResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterStorageResource.class);

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Context
    private UriInfo resourceURI;

    @Operation(
            description = "Count storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "The number of storage entries that match the given filters"
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
    @RequireAuthentication
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
    @Path("size")
    public Response countBundleInfo(@Parameter(description = "search by storage id parameter") @QueryParam("id") Long dataBundleId,
                                    @Parameter(description = "search by storage storage owner parameter") @QueryParam("ownerKey") String ownerKey,
                                    @Parameter(description = "search by storage storage host parameter") @QueryParam("storageAgent") String storageHost,
                                    @Parameter(description = "search by storage storage tags parameter") @QueryParam("storageTags") String storageTags,
                                    @Parameter(description = "search by storage storage volume parameter") @QueryParam("volumeName") String volumeName,
                                    @Context SecurityContext securityContext) {
        String dataOwnerKey;
        if (securityContext.isUserInRole(JacsCredentials.ADMIN)) {
            // if it's an admin use the owner param if set or allow it not to be set
            dataOwnerKey = StringUtils.defaultIfBlank(ownerKey, SecurityUtils.getUserPrincipal(securityContext).getSubjectKey());
        } else {
            // otherwise use the subject from the security context
            dataOwnerKey = SecurityUtils.getUserPrincipal(securityContext).getSubjectKey();
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .ownerKey(dataOwnerKey)
                .storageAgentId(storageHost)
                .storageTags(storageTags)
                .volumeName(volumeName)
                .build();
        LOG.info("Count storage records filtered with: {}", dataBundle);
        long nMatchingBundles = storageLookupService.countMatchingDataBundles(dataBundle);
        return Response
                .ok(nMatchingBundles)
                .build();
    }

    @Operation(
            description = "Retrieve storage entry by ID."
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
                    responseCode = "403",
                    description = "If user is authenticated but does not have the privileges"
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "If entry ID is invalid"
            ),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @RequireAuthentication
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Response getBundleInfo(@PathParam("id") Long id, @Context SecurityContext securityContext) {
        LOG.info("Retrieve storage info for {}", id);
        JacsBundle jacsBundle = storageLookupService.getDataBundleById(id);
        if (jacsBundle == null) {
            LOG.warn("No storage found for {}", id);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No storage found for " + id))
                    .build();
        } else if (securityContext.isUserInRole(JacsCredentials.ADMIN) ||
                jacsBundle.hasReadPermissions(SecurityUtils.getUserPrincipal(securityContext).getSubjectKey())) {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        } else {
            LOG.warn("Subject {} has no permissions to access storage {}", securityContext.getUserPrincipal(), jacsBundle);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No permissions to access " + id))
                    .build();
        }
    }

    @Operation(
            description = "List storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}."
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
    @RequireAuthentication
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response listBundleInfo(@QueryParam("id") Long dataBundleId,
                                   @QueryParam("name") String dataBundleName,
                                   @QueryParam("ownerKey") String ownerKey,
                                   @QueryParam("storageAgent") String storageAgent,
                                   @QueryParam("storageTags") List<String> storageTags,
                                   @QueryParam("volumeName") String volumeName,
                                   @QueryParam("storagePath") String storagePath,
                                   @QueryParam("page") Long pageNumber,
                                   @QueryParam("length") Integer pageLength,
                                   @Context SecurityContext securityContext) {
        String dataOwnerKey;
        if (securityContext.isUserInRole(JacsCredentials.ADMIN)) {
            // if it's an admin use the owner param if set or allow it not to be set
            dataOwnerKey = ownerKey;
        } else {
            // otherwise use the subject from the security context
            dataOwnerKey = SecurityUtils.getUserPrincipal(securityContext).getSubjectKey();
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .name(dataBundleName)
                .ownerKey(dataOwnerKey)
                .storageAgentId(storageAgent)
                .storageTagsAsList(storageTags)
                .volumeName(volumeName)
                .path(storagePath)
                .build();
        LOG.info("Count storage records filtered with: {}", dataBundle);
        PageRequest pageRequest = new PageRequestBuilder()
                .pageNumber(pageNumber)
                .pageSize(pageLength)
                .build();
        LOG.info("List storage records filtered with: {} / {}", dataBundle, pageRequest);
        PageResult<JacsBundle> dataBundleResults = storageLookupService.findMatchingDataBundles(dataBundle, pageRequest);
        PageResult<DataStorageInfo> results = new PageResult<>();
        results.setSortCriteria(dataBundleResults.getSortCriteria());
        results.setPageOffset(dataBundleResults.getPageOffset());
        results.setPageSize(dataBundleResults.getResultList().size());
        results.setResultList(dataBundleResults.getResultList().stream()
                .map(DataStorageInfo::fromBundle)
                .collect(Collectors.toList()));
        return Response
                .ok(results)
                .build();
    }

    @Operation(
            description = "Retrieve storage entry by owner and name."
    )
    @ApiResponses(value = {
            @ApiResponse(
                    responseCode = "200",
                    description = "The storage entry with the given name owned by the provided subject"
            ),
            @ApiResponse(
                    responseCode = "401",
                    description = "If user is not authenticated"
            ),
            @ApiResponse(
                    responseCode = "403",
                    description = "If user is authenticated, but does not have permissions to access the specified bundle"
            ),
            @ApiResponse(
                    responseCode = "404",
                    description = "If entry ID is invalid"
            ),
            @ApiResponse(
                    responseCode = "500",
                    description = "Data read error"
            )
    })
    @RequireAuthentication
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{ownerKey}/{name}")
    public Response getBundleInfoByOwnerAndName(@PathParam("ownerKey") String ownerKey,
                                                @PathParam("name") String name,
                                                @Context SecurityContext securityContext) {
        LOG.info("Retrieve storage info by owner and name for {} - {}", ownerKey, name);
        JacsBundle jacsBundle = storageLookupService.findDataBundleByOwnerKeyAndName(ownerKey, name);
        if (jacsBundle == null) {
            LOG.warn("No storage found for {} - {}", ownerKey, name);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No storage found for " + ownerKey + " with name " + name))
                    .build();
        } else if (securityContext.isUserInRole(JacsCredentials.ADMIN) ||
                jacsBundle.hasReadPermissions(SecurityUtils.getUserPrincipal(securityContext).getSubjectKey())) {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        } else {
            LOG.warn("Subject {} has no permissions to access storage {}", securityContext.getUserPrincipal(), jacsBundle);
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .entity(new ErrorResponse("No permissions to access " + ownerKey + "/" + name))
                    .build();
        }
    }

    @Operation(
            description = "Create new storage entry."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "The new storage entry."),
            @ApiResponse(responseCode = "401", description = "If user is not authenticated"),
            @ApiResponse(responseCode = "403", description = "If user is authenticated but does not have enough privileges to perform the operation"),
            @ApiResponse(responseCode = "404", description = "Volume on which to store the data was not found or no agent is available."),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "ALLOCATE_STORAGE_METADATA",
            argList = {0, 1}
    )
    @RequireAuthentication
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response createBundleInfo(@Parameter(description = "information about the storage to be created") DataStorageInfo dataStorageInfo,
                                     @Context SecurityContext securityContext) {
        LOG.info("Create storage: {} with credentials {}", dataStorageInfo, securityContext.getUserPrincipal());
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(dataStorageInfo.getPath(), dataBundle, SecurityUtils.getUserPrincipal(securityContext)
        );
        return dataBundleInfo
                .map(bi -> Response
                        .created(resourceURI.getBaseUriBuilder().path(dataBundle.getId().toString()).build())
                        .entity(DataStorageInfo.fromBundle(bi))
                        .build())
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("Metadata could not be created. Usually the reason is that no agent is available"))
                        .build());
    }

    @Operation(
            description = "Update a storage entry."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The updated storage entry."),
            @ApiResponse(responseCode = "401", description = "If user is not authenticated"),
            @ApiResponse(responseCode = "403", description = "If user is authenticated but does not have enough privileges to perform the operation"),
            @ApiResponse(responseCode = "404", description = "Invalid storage entry id."),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "UPDATE_STORAGE_METADATA",
            argList = {0, 1, 2}
    )
    @RequireAuthentication
    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Response updateBundleInfo(@PathParam("id") Long id, DataStorageInfo dataStorageInfo, @Context SecurityContext securityContext) {
        LOG.info("Update storage: {} - {}", id, dataStorageInfo);
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        dataBundle.setId(id);
        JacsBundle updatedDataBundleInfo = storageAllocatorService.updateStorage(dataBundle, SecurityUtils.getUserPrincipal(securityContext));
        if (updatedDataBundleInfo == null) {
            LOG.warn("Invalid storage ID: {} for updating {}", id, dataStorageInfo);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No storage found for " + id))
                    .build();
        } else {
            return Response
                    .ok(DataStorageInfo.fromBundle(updatedDataBundleInfo))
                    .build();
        }
    }

    @Operation(
            description = "Update a storage entry."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "The storage entry was removed."),
            @ApiResponse(responseCode = "401", description = "If user is not authenticated"),
            @ApiResponse(responseCode = "403", description = "If user is authenticated but does not have enough privileges to perform the operation"),
            @ApiResponse(responseCode = "404", description = "Invalid storage entry id."),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "DELETE_STORAGE_METADATA"
    )
    @RequireAuthentication
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{id}")
    public Response deleteBundleInfo(@PathParam("id") Long id, @Context SecurityContext securityContext) {
        LOG.info("Delete storage: {}", id);
        JacsBundle dataBundle = new JacsBundle();
        dataBundle.setId(id);
        if (storageAllocatorService.deleteStorage(dataBundle, SecurityUtils.getUserPrincipal(securityContext))) {
            LOG.info("Deleted storage: {}", id);
            return Response
                    .status(Response.Status.NO_CONTENT)
                    .build();
        } else {
            LOG.info("Storage {} was not deleted", id);
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
    }

}
