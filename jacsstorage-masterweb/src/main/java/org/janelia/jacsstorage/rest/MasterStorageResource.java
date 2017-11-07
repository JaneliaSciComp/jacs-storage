package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.security.JacsSecurityContext;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.annotation.security.PermitAll;
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
public class MasterStorageResource {

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Context
    private UriInfo resourceURI;

    @PermitAll
    @Produces(MediaType.TEXT_PLAIN)
    @GET
    @Path("status")
    public String getStatus() {
        return "OK";
    }

    @GET
    public Response listBundleInfo(@QueryParam("id") Long dataBundleId,
                                   @QueryParam("owner") String owner,
                                   @QueryParam("location") String dataLocation,
                                   @QueryParam("page") Long pageNumber,
                                   @QueryParam("length") Integer pageLength,
                                   @Context SecurityContext securityContext) {
        String dataOwner;
        if (securityContext.isUserInRole(JacsSecurityContext.ADMIN)) {
            // if it's an admin use the owner param if set or allow it not to be set
            dataOwner = owner;
        } else {
            // otherwise if the owner is not set use the subject from the security context
            if (StringUtils.isNotBlank(owner)) {
                dataOwner = owner;
            } else {
                dataOwner = SecurityUtils.getUserPrincipal(securityContext).getName();
            }
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .owner(dataOwner)
                .location(dataLocation)
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

    @GET
    @Path("{id}")
    public Response getBundleInfo(@PathParam("id") Long id, @Context SecurityContext securityContext) {
        JacsBundle jacsBundle = storageLookupService.getDataBundleById(id);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (SecurityUtils.getUserPrincipal(securityContext).getName().equals(jacsBundle.getOwner()) || securityContext.isUserInRole(JacsSecurityContext.ADMIN)) {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        } else {
            return Response
                    .status(Response.Status.FORBIDDEN)
                    .build();
        }
    }

    @GET
    @Path("{owner}/{name}")
    public Response getBundleInfoByOwnerAndName(@PathParam("owner") String owner,
                                                @PathParam("name") String name,
                                                @Context SecurityContext securityContext) {
        JacsBundle jacsBundle = storageLookupService.findDataBundleByOwnerAndName(owner, name);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else if (SecurityUtils.getUserPrincipal(securityContext).getName().equals(jacsBundle.getOwner()) || securityContext.isUserInRole(JacsSecurityContext.ADMIN)) {
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
    @Consumes("application/json")
    @POST
    public Response createBundleInfo(DataStorageInfo dataStorageInfo, @Context SecurityContext securityContext) {
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
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
