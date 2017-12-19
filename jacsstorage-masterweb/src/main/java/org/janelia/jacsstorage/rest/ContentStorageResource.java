package org.janelia.jacsstorage.rest;

import com.google.common.io.ByteStreams;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.utils.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage-content")
public class ContentStorageResource {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageResource.class);

    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("path/{filePath:.+}")
    public Response getContentStream(@PathParam("filePath") String fullFileNameParam, @Context SecurityContext securityContext) {
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageLookupService, storageVolumeManager);
        return storageResourceHelper.retrieveFileContent(
                fullFileNameParam,
                null, // if more than one volume can serve the file just pick up the first
                (dataBundle, dataEntryPath) -> retrieveFileFromBundle(dataBundle, dataEntryPath, SecurityUtils.getUserPrincipal(securityContext)),
                (storageVolume, dataEntryPath) -> retrieveFileFromVolume(storageVolume, dataEntryPath, SecurityUtils.getUserPrincipal(securityContext))
        );
    }

    private Response retrieveFileFromBundle(JacsBundle dataBundle, String dataEntryPath, JacsCredentials jacsCredentials) {
        return dataBundle.getStorageVolume()
                .map(storageVolume -> {
                    StreamingOutput stream = streamFromURL(
                            storageVolume.getStorageServiceURL() + "/agent-api/agent-storage/" + dataBundle.getId() + "/entry-content/" + dataEntryPath,
                            jacsCredentials);
                    return Response
                            .ok(stream, MediaType.APPLICATION_OCTET_STREAM)
                            .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName() + "/" + dataEntryPath)
                            .build();
                })
                .orElse(Response
                        .status(Response.Status.BAD_REQUEST.getStatusCode())
                        .entity(new ErrorResponse("No volume associated with databundle " + dataBundle.getId()))
                        .build())
        ;
    }

    private Response retrieveFileFromVolume(JacsStorageVolume storageVolume, java.nio.file.Path dataEntryPath, JacsCredentials jacsCredentials) {
        String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
        StreamingOutput stream = streamFromURL(
                            storageServiceURL + "agent-storage/storageVolume/" + storageVolume.getId() + "/" + dataEntryPath,
                            jacsCredentials);
        return Response
                .ok(stream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + storageVolume.getId() + "/" + dataEntryPath)
                .build();
    }

    private StreamingOutput streamFromURL(String url, JacsCredentials jacsCredentials) {
        return output -> {
            Client httpClient = null;
            try {
                httpClient = HttpUtils.createHttpClient();
                WebTarget target = httpClient.target(url);
                Response response = target.request(MediaType.APPLICATION_OCTET_STREAM_TYPE)
                        .header("Authorization", "Bearer " + jacsCredentials.getAuthToken())
                        .header("JacsSubject", jacsCredentials.getAuthSubject())
                        .get();
                int responseStatus = response.getStatus();
                if (responseStatus == Response.Status.OK.getStatusCode()) {
                    InputStream stream = response.readEntity(InputStream.class);
                    ByteStreams.copy(stream, output);
                    output.flush();
                } else {
                    LOG.warn("{} returned {} status", url, responseStatus);
                    throw new WebApplicationException(responseStatus);
                }
            } catch (WebApplicationException e) {
                throw e;
            } catch (Exception e) {
                LOG.error("Error streaming data from {}", url, e);
                throw new WebApplicationException(e);
            } finally {
                if (httpClient != null) {
                    httpClient.close();
                }
            }
        };
    }

}
