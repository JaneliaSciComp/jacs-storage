package org.janelia.jacsstorage.rest;

import com.google.common.io.ByteStreams;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.serviceutils.HttpUtils;
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
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import java.io.InputStream;

@Timed
@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage_content")
@Api(value = "File path based API for retrieving storage content")
public class ContentStorageResource {
    private static final Logger LOG = LoggerFactory.getLogger(ContentStorageResource.class);

    @Inject @RemoteInstance
    private StorageLookupService storageLookupService;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    /**
     * Retrieve the content of a file using the file path.
     *
     * @param fullFileNameParam
     * @param securityContext
     * @return
     */
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_path/{filePath:.+}")
    @ApiOperation(value = "Get file content", notes = "")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Success", response = StreamingOutput.class),
            @ApiResponse(code = 404, message = "Specified file path not found", response = ErrorResponse.class)
    })
    public Response getContentStream(@PathParam("filePath") String fullFileNameParam, @Context SecurityContext securityContext) {
        LOG.info("Stream content of {}", fullFileNameParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(null, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                fullFileNameParam,
                (dataBundle, dataEntryPath) -> retrieveFileFromBundle(dataBundle, dataEntryPath, SecurityUtils.getUserPrincipal(securityContext)),
                (storageVolume, dataEntryPath) -> retrieveFileFromVolume(storageVolume, dataEntryPath, SecurityUtils.getUserPrincipal(securityContext))
        );
    }

    private Response retrieveFileFromBundle(JacsBundle dataBundle, String dataEntryPath, JacsCredentials jacsCredentials) {
        LOG.info("Retrieve file {} from storage {}", dataEntryPath, dataBundle);
        return dataBundle.getStorageVolume()
                .map(storageVolume -> {
                    StreamingOutput stream = streamFromURL(
                            storageVolume.getStorageServiceURL() + "/agent_api/agent_storage/" + dataBundle.getId() + "/entry_content/" + dataEntryPath,
                            jacsCredentials);
                    return Response
                            .ok(stream, MediaType.APPLICATION_OCTET_STREAM)
                            .header("content-disposition","attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName() + "/" + dataEntryPath)
                            .build();
                })
                .orElse(Response
                        .status(Response.Status.BAD_REQUEST.getStatusCode())
                        .entity(new ErrorResponse("No volume associated with databundle " + dataBundle.getId()))
                        .build())
        ;
    }

    private Response retrieveFileFromVolume(JacsStorageVolume storageVolume, String dataEntryPath, JacsCredentials jacsCredentials) {
        LOG.info("Retrieve file {} from volume {}", dataEntryPath, storageVolume);
        String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
        StreamingOutput stream = streamFromURL(
                            storageServiceURL + "agent_storage/storage_volume/" + storageVolume.getId() + "/" + dataEntryPath,
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
                target.request(MediaType.APPLICATION_OCTET_STREAM);

                Response response =
                        createRequestWithCredentials(target.request(
                                MediaType.APPLICATION_OCTET_STREAM_TYPE),
                                jacsCredentials)
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

    Invocation.Builder createRequestWithCredentials(Invocation.Builder requestBuilder, JacsCredentials jacsCredentials) {
        Invocation.Builder requestWithCredentialsBuilder = requestBuilder;
        if (jacsCredentials.hasAuthToken()) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "Authorization",
                    "Bearer " + jacsCredentials.getAuthToken());
        }
        if (jacsCredentials.hasAuthSubject()) {
            requestWithCredentialsBuilder = requestWithCredentialsBuilder.header(
                    "JacsSubject",
                    jacsCredentials.getAuthSubject());
        }
        return requestWithCredentialsBuilder;
    }
}
