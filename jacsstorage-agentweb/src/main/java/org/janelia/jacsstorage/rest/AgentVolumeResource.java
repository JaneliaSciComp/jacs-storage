package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.StreamingOutput;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.List;

@Timed
@RequestScoped
@RequireAuthentication
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "jwtBearerToken", name = "Authorization", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(
        value = "Agent storage volumes API. This API requires an authenticated subject.",
        authorizations = {
                @Authorization("jwtBearerToken")
        }
)
public class AgentVolumeResource {

    private static final Logger LOG = LoggerFactory.getLogger(AgentVolumeResource.class);

    @Inject @LocalInstance
    private StorageUsageManager storageUsageManager;

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{storageVolumeId}/report/{subjectName:\\w*}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveSubjectQuota(@PathParam("storageVolumeId") Long storageVolumeId,
                                         @PathParam("subjectName") String subjectName,
                                         @Context SecurityContext securityContext) {
        LOG.info("Retrieve user quota for {} on {}", subjectName, storageVolumeId);
        List<UsageData> usageData;
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getVolumeUsage(storageVolumeId,
                    SecurityUtils.getUserPrincipal(securityContext));
        } else {
            UsageData subjectUsageData = storageUsageManager.getVolumeUsageForUser(storageVolumeId,
                    subjectName,
                    SecurityUtils.getUserPrincipal(securityContext));
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }


}
