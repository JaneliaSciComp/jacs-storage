package org.janelia.jacsstorage.rest.nonauthenticated;

import com.google.common.collect.ImmutableList;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiKeyAuthDefinition;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.SecurityDefinition;
import io.swagger.annotations.SwaggerDefinition;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Api(value = "Agent storage volumes API.")
@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class VolumeQuotaResource {

    private static final Logger LOG = LoggerFactory.getLogger(VolumeQuotaResource.class);

    @Inject @LocalInstance
    private StorageUsageManager storageUsageManager;

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{volumeName}/status")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getSubjectQuotaStatusForVolumeName(@PathParam("volumeName") String volumeName,
                                                       @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve usage status for {} on {}", subjectName, volumeName);
        return retrieveQuotaForVolumeNameAndSubject(volumeName, subjectName);
    }

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{volumeName}/report/{subjectName}")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getSubjectReportQuotaForVolumeName(@PathParam("volumeName") String volumeName,
                                                       @PathParam("subjectName") String subjectNameParam) {
        LOG.info("Retrieve quota(s) for {} on {}", subjectNameParam, volumeName);
        String subjectName = JacsSubjectHelper.getNameFromSubjectKey(subjectNameParam);
        if (StringUtils.isBlank(subjectName)) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("Subject name parameter is required"))
                    .build();
        } else {
            UsageData subjectUsageData = storageUsageManager.getUsageByVolumeNameForUser(volumeName, subjectName);
            return Response
                    .ok(subjectUsageData)
                    .build();
        }
    }

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("quota/{volumeName}/report")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
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

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("volume_quota/{storageVolumeId}/report")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
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

    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("path_quota/{dataPath:.+}/report")
    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
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
