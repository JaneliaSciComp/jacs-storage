package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Base64;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("agent-storage")
public class AgentStorageResource {

    private static final long _1_K = 1024;

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
                                  InputStream bundleStream) throws IOException {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        TransferInfo ti = dataStorageService.persistDataStream(dataBundle.getPath(), dataBundle.getStorageFormat(), bundleStream);
        dataBundle.setChecksum(Base64.getEncoder().encodeToString(ti.getChecksum()));
        dataBundle.setUsedSpaceInKB(ti.getNumBytes() / _1_K);
        storageAllocatorService.updateStorage(dataBundle);
        return Response
                .ok(DataStorageInfo.fromBundle(dataBundle))
                .build();
    }

    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    @GET
    @Path("{dataBundleId}")
    public Response retrieveStream(@PathParam("dataBundleId") Long dataBundleId) {
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

    @DELETE
    @Path("{dataBundleId}")
    public Response deleteStorage(@PathParam("dataBundleId") Long dataBundleId) throws IOException {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        dataStorageService.deleteStorage(dataBundle.getPath());
        dataStorageService.cleanupStoragePath(Paths.get(dataBundle.getPath()).getParent().toString());
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
