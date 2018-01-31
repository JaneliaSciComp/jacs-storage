package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.security.JacsSecurityContext;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import java.util.Optional;
import java.util.stream.Collectors;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
@Api(value = "Master storage API.")
public class MasterStorageResource {

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Context
    private UriInfo resourceURI;

    @Produces(MediaType.TEXT_PLAIN)
    @GET
    @Path("status")
    @ApiOperation(value = "Retrieve master status.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "If the server is up and running")
    })
    public String getStatus() {
        return "OK";
    }

    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
    @RequireAuthentication
    @GET
    @Path("size")
    @ApiOperation(value = "Count storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The number of storage entries that match the given filters"),
            @ApiResponse(code = 401, message = "If user is not authenticated"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response countBundleInfo(@QueryParam("id") Long dataBundleId,
                                    @QueryParam("ownerKey") String ownerKey,
                                    @QueryParam("storageHost") String storageHost,
                                    @QueryParam("storageTags") String storageTags,
                                    @QueryParam("volumeName") String volumeName,
                                    @Context SecurityContext securityContext) {
        String dataOwnerKey;
        if (securityContext.isUserInRole(JacsSecurityContext.ADMIN)) {
            // if it's an admin use the owner param if set or allow it not to be set
            dataOwnerKey = StringUtils.defaultIfBlank(ownerKey, SecurityUtils.getUserPrincipal(securityContext).getSubjectKey());
        } else {
            // otherwise use the subject from the security context
            dataOwnerKey = SecurityUtils.getUserPrincipal(securityContext).getSubjectKey();
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .ownerKey(dataOwnerKey)
                .storageHost(storageHost)
                .storageTags(storageTags)
                .volumeName(volumeName)
                .build();
        long nMatchingBundles = storageLookupService.countMatchingDataBundles(dataBundle);
        return Response
                .ok(nMatchingBundles)
                .build();
    }

    @RequireAuthentication
    @GET
    @ApiOperation(value = "List storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The list of storage entries that match the given filters"),
            @ApiResponse(code = 401, message = "If user is not authenticated"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response listBundleInfo(@QueryParam("id") Long dataBundleId,
                                   @QueryParam("ownerKey") String ownerKey,
                                   @QueryParam("storageHost") String storageHost,
                                   @QueryParam("storageTags") String storageTags,
                                   @QueryParam("volumeName") String volumeName,
                                   @QueryParam("page") Long pageNumber,
                                   @QueryParam("length") Integer pageLength,
                                   @Context SecurityContext securityContext) {
        String dataOwnerKey;
        if (securityContext.isUserInRole(JacsSecurityContext.ADMIN)) {
            // if it's an admin use the owner param if set or allow it not to be set
            dataOwnerKey = StringUtils.defaultIfBlank(ownerKey, SecurityUtils.getUserPrincipal(securityContext).getSubjectKey());
        } else {
            // otherwise use the subject from the security context
            dataOwnerKey = SecurityUtils.getUserPrincipal(securityContext).getSubjectKey();
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .ownerKey(dataOwnerKey)
                .storageHost(storageHost)
                .storageTags(storageTags)
                .volumeName(volumeName)
                .build();
        PageRequest pageRequest = new PageRequestBuilder()
                .pageNumber(pageNumber)
                .pageSize(pageLength)
                .build();
        PageResult<JacsBundle> dataBundleResults = storageLookupService.findMatchingDataBundles(dataBundle, pageRequest);
        PageResult<DataStorageInfo> results = new PageResult<>();
        results.setPageOffset(dataBundleResults.getPageOffset());
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

    @RequireAuthentication
    @GET
    @Path("{id}")
    @ApiOperation(value = "Retrieve storage entry by ID.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The storage entry with the given ID"),
            @ApiResponse(code = 401, message = "If user is not authenticated"),
            @ApiResponse(code = 404, message = "If entry ID is invalid"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getBundleInfo(@PathParam("id") Long id, @Context SecurityContext securityContext) {
        JacsBundle jacsBundle = storageLookupService.getDataBundleById(id);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (jacsBundle.hasReadPermissions(SecurityUtils.getUserPrincipal(securityContext).getSubjectKey())) {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        } else {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @RequireAuthentication
    @GET
    @Path("{ownerKey}/{name}")
    @ApiOperation(value = "Retrieve storage entry by owner and name.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The storage entry with the given name owned by the provided subject"),
            @ApiResponse(code = 401, message = "If user is not authenticated"),
            @ApiResponse(code = 404, message = "If entry ID is invalid"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getBundleInfoByOwnerAndName(@PathParam("ownerKey") String ownerKey,
                                                @PathParam("name") String name,
                                                @Context SecurityContext securityContext) {
        JacsBundle jacsBundle = storageLookupService.findDataBundleByOwnerKeyAndName(ownerKey, name);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (jacsBundle.hasReadPermissions(SecurityUtils.getUserPrincipal(securityContext).getSubjectKey())) {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        } else {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @LogStorageEvent(
            eventName = "ALLOCATE_STORAGE_METADATA"
    )
    @RequireAuthentication
    @Consumes("application/json")
    @POST
    @ApiOperation(value = "Create new storage entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The new storage entry."),
            @ApiResponse(code = 401, message = "If user is not authenticated"),
            @ApiResponse(code = 404, message = "Volume on which to store the data was not found or no agent is available."),
            @ApiResponse(code = 500, message = "Data write error")
    })
    public Response createBundleInfo(DataStorageInfo dataStorageInfo, @Context SecurityContext securityContext) {
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(SecurityUtils.getUserPrincipal(securityContext),
                dataStorageInfo.getStorageRootPrefixDir(),
                dataBundle);
        return dataBundleInfo
                .map(bi -> Response
                        .created(resourceURI.getBaseUriBuilder().path(dataBundle.getId().toString()).build())
                        .entity(DataStorageInfo.fromBundle(bi))
                        .build())
                .orElse(Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ImmutableMap.of("errormessage", "Metadata could not be created. Usually the reason is that no agent is available"))
                        .build());
    }

    @LogStorageEvent(
            eventName = "UPDATE_STORAGE_METADATA"
    )
    @RequireAuthentication
    @Consumes("application/json")
    @PUT
    @Path("{id}")
    public Response updateBundleInfo(@PathParam("id") Long id, DataStorageInfo dataStorageInfo, @Context SecurityContext securityContext) {
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        dataBundle.setId(id);
        JacsBundle updatedDataBundleInfo = storageAllocatorService.updateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
        if (updatedDataBundleInfo == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .ok(DataStorageInfo.fromBundle(updatedDataBundleInfo))
                    .build();
        }
    }

    @LogStorageEvent(
            eventName = "DELETE_STORAGE_METADATA"
    )
    @RequireAuthentication
    @DELETE
    @Path("{id}")
    public Response deleteBundleInfo(@PathParam("id") Long id, @Context SecurityContext securityContext) {
        JacsBundle dataBundle = new JacsBundle();
        dataBundle.setId(id);
        if (storageAllocatorService.deleteStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle)) {
            return Response
                    .status(Response.Status.NO_CONTENT)
                    .build();
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        }
    }

}
