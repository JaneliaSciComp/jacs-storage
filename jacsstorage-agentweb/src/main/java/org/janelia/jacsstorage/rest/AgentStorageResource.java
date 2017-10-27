package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agent-storage")
public class AgentStorageResource {

    private static int MAX_ALLOWED_DEPTH = 20;

    @Inject
    private DataStorageService dataStorageService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;

    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @POST
    @Path("{dataBundleId}")
    public Response persistStream(@PathParam("dataBundleId") Long dataBundleId,
                                  @Context SecurityContext securityContext,
                                  InputStream bundleStream) throws IOException {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        TransferInfo ti = dataStorageService.persistDataStream(dataBundle.getPath(), dataBundle.getStorageFormat(), bundleStream);
        dataBundle.setChecksum(Base64.getEncoder().encodeToString(ti.getChecksum()));
        dataBundle.setUsedSpaceInBytes(ti.getNumBytes());
        storageAllocatorService.updateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
        return Response
                .ok(DataStorageInfo.fromBundle(dataBundle))
                .build();
    }

    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @GET
    @Path("{dataBundleId}")
    public Response retrieveStream(@PathParam("dataBundleId") Long dataBundleId,
                                   @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        StreamingOutput bundleStream =  new StreamingOutput()
        {
            @Override
            public void write(java.io.OutputStream output) throws IOException, WebApplicationException {
                try {
                    dataStorageService.retrieveDataStream(dataBundle.getPath(), dataBundle.getStorageFormat(), output);
                    output.flush();
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName())
                .build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("{dataBundleId}/list")
    public Response listContent(@PathParam("dataBundleId") Long dataBundleId,
                                @QueryParam("depth") Integer depthParam,
                                @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        int depth = depthParam != null && depthParam >= 0 && depthParam < MAX_ALLOWED_DEPTH ? depthParam : MAX_ALLOWED_DEPTH;
        List<DataNodeInfo> dataBundleCotent = dataStorageService.listDataEntries(dataBundle.getPath(), dataBundle.getStorageFormat(), depth);
        return Response
                .ok(dataBundleCotent, MediaType.APPLICATION_JSON)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName())
                .build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("{dataBundleId}/entry-content/{dataEntryPath: .*}")
    public Response entryContent(@PathParam("dataBundleId") Long dataBundleId,
                                @PathParam("dataEntryPath") String dataEntryPath,
                                @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        StreamingOutput bundleStream =  new StreamingOutput()
        {
            @Override
            public void write(java.io.OutputStream output) throws IOException, WebApplicationException {
                try {
                    ByteStreams.copy(
                            dataStorageService.getDataEntryStream(dataBundle.getPath(), dataEntryPath, dataBundle.getStorageFormat()),
                            output);
                    output.flush();
                } catch (Exception e) {
                    throw new WebApplicationException(e);
                }
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName() + "/" + dataEntryPath)
                .build();
    }

    @DELETE
    @Path("{dataBundleId}")
    public Response deleteStorage(@PathParam("dataBundleId") Long dataBundleId,
                                  @Context SecurityContext securityContext) throws IOException {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        if (dataBundle != null) {
            dataStorageService.deleteStorage(dataBundle.getPath());
            dataStorageService.cleanupStoragePath(Paths.get(dataBundle.getPath()).getParent().toString());
        }
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
