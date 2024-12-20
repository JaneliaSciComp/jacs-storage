package org.janelia.jacsstorage.rest;

import java.util.List;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import com.google.common.collect.ImmutableList;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "VolumeQuota", description = "Agent storage volumes API.")
@Timed
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class VolumeQuotaResource {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeQuotaResource.class);

    @Inject @LocalInstance
    private StorageUsageManager storageUsageManager;

    @Operation(description = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/status")
    public Response getQuotaStatusForVolumeName(@PathParam("volumeName") String volumeName,
                                                @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve usage status for {} on {}", subjectName, volumeName);
        return retrieveQuotaForVolumeNameAndSubject(volumeName, subjectName);
    }

    @Operation(description = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/report")
    public Response getReportQuotaForVolumeName(@PathParam("volumeName") String volumeName,
                                                @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve quota(s) for {} on {}", subjectName, volumeName);
        return retrieveQuotaForVolumeNameAndSubject(volumeName, subjectName);
    }

    private Response retrieveQuotaForVolumeNameAndSubject(String volumeName, String subjectNameParam) {
        List<UsageData> usageData;
        String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subjectNameParam);
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByVolumeName(volumeName);
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeNameForUser(volumeName, subjectName);
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

    @Operation(description = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/report/{subjectName}")
    public Response getReportQuotaForVolumeNameAndSubject(@PathParam("volumeName") String volumeName,
                                                          @PathParam("subjectName") String subjectName) {
        LOG.info("Retrieve quota for {} on {}", subjectName, volumeName);
        if (StringUtils.isBlank(subjectName)) {
            return Response
                    .status(Response.Status.BAD_REQUEST)
                    .entity(new ErrorResponse("Subject name must be specified"))
                    .build();
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeNameForUser(volumeName, subjectName);
            return Response
                    .ok(subjectUsageData)
                    .build();
        }
    }

    @Operation(description = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("volume_quota/{storageVolumeId}/report")
    public Response getQuotaReportForVolumeId(@PathParam("storageVolumeId") Long storageVolumeId,
                                              @QueryParam("subjectName") String subjectNameParam) {
        LOG.info("Retrieve user quota for {} on {}", subjectNameParam, storageVolumeId);
        List<UsageData> usageData;
        String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subjectNameParam);
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByVolumeId(storageVolumeId);
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeIdForUser(storageVolumeId, subjectName);
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

    @Operation(description = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "The stream was successfull"),
            @ApiResponse(responseCode = "404", description = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("path_quota/{dataPath:.+}/report")
    public Response getQuotaReportForDataPath(@PathParam("dataPath") String dataPath,
                                              @QueryParam("subjectName") String subjectNameParam) {
        String fullDataPath = StringUtils.prependIfMissing(dataPath, "/");
        LOG.info("Retrieve user quota for {} on {}", subjectNameParam, fullDataPath);
        List<UsageData> usageData;
        String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subjectNameParam);
        if (StringUtils.isBlank(subjectName)) {
            usageData = storageUsageManager.getUsageByStoragePath(fullDataPath);
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByStoragePathForUser(fullDataPath, subjectName);
            usageData = ImmutableList.of(subjectUsageData);
        }
        return Response
                .ok(usageData)
                .build();
    }

}
