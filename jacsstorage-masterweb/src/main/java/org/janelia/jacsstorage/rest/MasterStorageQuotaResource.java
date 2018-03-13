package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableList;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import java.util.List;

@Timed
@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path("storage")
@SwaggerDefinition(
        securityDefinition = @SecurityDefinition(
                apiKeyAuthDefinitions = {
                        @ApiKeyAuthDefinition(key = "jwtBearerToken", name = "Authorization", in = ApiKeyAuthDefinition.ApiKeyLocation.HEADER)
                }
        )
)
@Api(value = "Master storage quota API.")
public class MasterStorageQuotaResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterStorageQuotaResource.class);

    @Inject @RemoteInstance
    private StorageUsageManager storageUsageManager;

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{volumeName}/status/{subjectName:\\w*}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveSubjectQuotaStatusForVolumeName(@PathParam("volumeName") String volumeName,
                                                            @PathParam("subjectName") String subjectName,
                                                            @Context SecurityContext securityContext) {
        LOG.info("Retrieve usage status for {} on {}", subjectName, volumeName);
        return retrieveSubjectQuotaStatusForVolumeName(volumeName, subjectName, SecurityUtils.getUserPrincipal(securityContext));
    }

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{volumeName}/report/{subjectName:\\w*}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveSubjectQuotaForVolumeName(@PathParam("volumeName") String volumeName,
                                                      @PathParam("subjectName") String subjectName,
                                                      @Context SecurityContext securityContext) {
        LOG.info("Retrieve quota(s) for {} on {}", subjectName, volumeName);
        return retrieveSubjectQuotaStatusForVolumeName(volumeName, subjectName, SecurityUtils.getUserPrincipal(securityContext));
    }

    private Response retrieveSubjectQuotaStatusForVolumeName(String volumeName, String subjectName, JacsCredentials userPrincipal) {
        List<UsageData> usageData;
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByVolumeName(volumeName, userPrincipal);
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeNameForUser(volumeName, subjectName, userPrincipal);
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("volume_quota/{storageVolumeId}/report/{subjectName:\\w*}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveSubjectQuotaForVolumeId(@PathParam("storageVolumeId") Long storageVolumeId,
                                                    @PathParam("subjectName") String subjectName,
                                                    @Context SecurityContext securityContext) {
        LOG.info("Retrieve user quota for {} on {}", subjectName, storageVolumeId);
        List<UsageData> usageData;
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByVolumeId(storageVolumeId,
                    SecurityUtils.getUserPrincipal(securityContext));
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeIdForUser(storageVolumeId,
                    subjectName,
                    SecurityUtils.getUserPrincipal(securityContext));
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("path_quota/{dataPath:.+}/report/{subjectName:\\w*}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveSubjectQuotaForDataPath(@PathParam("dataPath") String dataPath,
                                                    @PathParam("subjectName") String subjectName,
                                                    @Context SecurityContext securityContext) {
        String fullDataPath = StringUtils.prependIfMissing(dataPath, "/");
        LOG.info("Retrieve user quota for {} on {}", subjectName, fullDataPath);
        List<UsageData> usageData;
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByStoragePath(fullDataPath,
                    SecurityUtils.getUserPrincipal(securityContext));
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByStoragePathForUser(fullDataPath,
                    subjectName,
                    SecurityUtils.getUserPrincipal(securityContext));
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

}
