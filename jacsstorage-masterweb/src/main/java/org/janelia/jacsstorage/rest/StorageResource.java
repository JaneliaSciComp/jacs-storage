package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.PageRequest;
import org.janelia.jacsstorage.datarequest.PageRequestBuilder;
import org.janelia.jacsstorage.datarequest.PageResult;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.service.StorageManagementService;

import javax.enterprise.context.RequestScoped;
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
import javax.ws.rs.core.UriInfo;
import java.util.Optional;
import java.util.stream.Collectors;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
public class StorageResource {

    @Inject
    private StorageManagementService storageManagementService;
    @Context
    private UriInfo resourceURI;

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
                                   @QueryParam("length") Integer pageLength) {
        JacsBundle dataBundle = new JacsBundleBuilder()
                .dataBundleId(dataBundleId)
                .owner(owner)
                .location(dataLocation)
                .build();
        PageRequest pageRequest = new PageRequestBuilder()
                .pageNumber(pageNumber)
                .pageSize(pageLength)
                .build();
        PageResult<JacsBundle> dataBundleResults = storageManagementService.findMatchingDataBundles(dataBundle, pageRequest);
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
    public Response getBundleInfo(@PathParam("id") Long id) {
        JacsBundle jacsBundle = storageManagementService.getDataBundleById(id);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        }
    }

    @GET
    @Path("{owner}/{name}")
    public Response getBundleInfoByOwnerAndName(@PathParam("owner") String owner, @PathParam("name") String name) {
        JacsBundle jacsBundle = storageManagementService.findDataBundleByOwnerAndName(owner, name);
        if (jacsBundle == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .build();
        } else {
            return Response
                    .ok(DataStorageInfo.fromBundle(jacsBundle))
                    .build();
        }
    }

    @Consumes("application/json")
    @POST
    public Response createBundleInfo(DataStorageInfo dataStorageInfo) {
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();

        Optional<JacsBundle> dataBundleInfo = storageManagementService.allocateStorage(dataBundle);
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

    @Consumes("application/json")
    @PUT
    @Path("{id}")
    public Response updateBundleInfo(@PathParam("id") Long id, DataStorageInfo dataStorageInfo) {
        JacsBundle dataBundle = dataStorageInfo.asDataBundle();
        dataBundle.setId(id);
        JacsBundle updatedDataBundleInfo = storageManagementService.updateDataBundle(dataBundle);
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

}
