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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "jwtBearerToken", name = "Authorization", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(value = "Master storage API.")
public class MasterStorageResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterStorageResource.class);

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Context
    private UriInfo resourceURI;

    @Produces({MediaType.APPLICATION_JSON, MediaType.TEXT_HTML})
    @RequireAuthentication
    @GET
    @Path("size")
    @ApiOperation(
            value = "Count storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}.",
            authorizations = {
                @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The number of storage entries that match the given filters",
                    response = Long.class
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
    public Response countBundleInfo(@ApiParam(value = "search by storage id parameter") @QueryParam("id") Long dataBundleId,
                                    @ApiParam(value = "search by storage storage owner parameter") @QueryParam("ownerKey") String ownerKey,
                                    @ApiParam(value = "search by storage storage host parameter") @QueryParam("storageHost") String storageHost,
                                    @ApiParam(value = "search by storage storage tags parameter") @QueryParam("storageTags") String storageTags,
                                    @ApiParam(value = "search by storage storage volume parameter") @QueryParam("volumeName") String volumeName,
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
                .storageHost(storageHost)
                .storageTags(storageTags)
                .volumeName(volumeName)
                .build();
        LOG.info("Count storage records filtered with: {}", dataBundle);
        long nMatchingBundles = storageLookupService.countMatchingDataBundles(dataBundle);
        return Response
                .ok(nMatchingBundles)
                .build();
    }

    @RequireAuthentication
    @GET
    @Path("{id}")
    @ApiOperation(
            value = "Retrieve storage entry by ID.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The storage entry with the given ID",
                    response = DataStorageInfo.class
            ),
            @ApiResponse(
                    code = 401,
                    message = "If user is not authenticated",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 403,
                    message = "If user is authenticated but does not have the privileges",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 404,
                    message = "If entry ID is invalid",
                    response = ErrorResponse.class
            ),
            @ApiResponse(code = 500, message = "Data read error")
    })
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

    @RequireAuthentication
    @GET
    @ApiOperation(
            value = "List storage entries. The entries could be filtered by {id, ownerKey, storageHost, storageTags, volumeName}.",
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
    public Response listBundleInfo(@QueryParam("id") Long dataBundleId,
                                   @QueryParam("name") String dataBundleName,
                                   @QueryParam("ownerKey") String ownerKey,
                                   @QueryParam("storageHost") String storageHost,
                                   @QueryParam("storageTags") List<String> storageTags,
                                   @QueryParam("volumeName") String volumeName,
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
                .storageHost(storageHost)
                .storageTagsAsList(storageTags)
                .volumeName(volumeName)
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

    @RequireAuthentication
    @GET
    @Path("{ownerKey}/{name}")
    @ApiOperation(
            value = "Retrieve storage entry by owner and name.",
            authorizations = {
                @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(
                    code = 200,
                    message = "The storage entry with the given name owned by the provided subject",
                    response = DataStorageInfo.class
            ),
            @ApiResponse(
                    code = 401,
                    message = "If user is not authenticated",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 403,
                    message = "If user is authenticated, but does not have permissions to access the specified bundle",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 404,
                    message = "If entry ID is invalid",
                    response = ErrorResponse.class
            ),
            @ApiResponse(
                    code = 500,
                    message = "Data read error",
                    response = ErrorResponse.class
            )
    })
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

    @LogStorageEvent(
            eventName = "ALLOCATE_STORAGE_METADATA",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Consumes("application/json")
    @POST
    @ApiOperation(
            value = "Create new storage entry.",
            authorizations = {
                    @Authorization("jwtBearerToken")
            }
    )
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The new storage entry.", response = DataStorageInfo.class),
            @ApiResponse(code = 401, message = "If user is not authenticated", response = ErrorResponse.class),
            @ApiResponse(code = 403, message = "If user is authenticated but does not have enough privileges to perform the operation", response = ErrorResponse.class),
            @ApiResponse(code = 404, message = "Volume on which to store the data was not found or no agent is available.", response = ErrorResponse.class),
            @ApiResponse(code = 500, message = "Data write error", response = ErrorResponse.class)
    })
    public Response createBundleInfo(@ApiParam(value = "information about the storage to be created") DataStorageInfo dataStorageInfo,
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

    @LogStorageEvent(
            eventName = "UPDATE_STORAGE_METADATA",
            argList = {0, 1, 2}
    )
    @RequireAuthentication
    @Consumes("application/json")
    @PUT
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

    @LogStorageEvent(
            eventName = "DELETE_STORAGE_METADATA"
    )
    @RequireAuthentication
    @DELETE
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
