package org.janelia.jacsstorage.rest;

import com.google.common.collect.ImmutableList;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.UsageData;
import org.janelia.jacsstorage.service.StorageUsageManager;
import org.janelia.jacsstorage.service.StorageVolumeManager;
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
import java.util.Map;
import java.util.stream.Collectors;

@Api(value = "Master storage quota API.")
@Timed
@Path("storage")
public class MasterStorageQuotaResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterStorageQuotaResource.class);

    @Inject @RemoteInstance
    private StorageUsageManager storageUsageManager;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    @ApiOperation(value = "Retrieve a user's quota on all storage volumes.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota_report")
    public Response getSubjectQuotaForAllVolumes(@QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve all quota(s) for {}", subjectName);
        StorageQuery storageQuery = new StorageQuery()
                .setIncludeInactiveVolumes(false);
        List<JacsStorageVolume> storageVolumes = storageVolumeManager.findVolumes(storageQuery);
        if (StringUtils.isBlank(subjectName)) {
            Map<String, List<UsageData>> usageDataMap = storageVolumes.stream()
                    .map(sv -> ImmutablePair.of(
                            sv.getName(),
                            storageUsageManager.getUsageByVolumeName(sv.getName())))
                    .collect(Collectors.toMap(vqp -> vqp.getLeft(), vqp -> vqp.getRight()));
            return Response
                    .ok(usageDataMap)
                    .build();
        } else {
            Map<String, UsageData> usageDataMap = storageVolumes.stream()
                    .map(sv -> ImmutablePair.of(
                            sv.getName(),
                            storageUsageManager.getUsageByVolumeNameForUser(sv.getName(), subjectName)))
                    .collect(Collectors.toMap(vqp -> vqp.getLeft(), vqp -> vqp.getRight()));
            return Response
                    .ok(usageDataMap)
                    .build();
        }
    }

    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/status")
    public Response getQuotaStatusForVolumeName(@PathParam("volumeName") String volumeName,
                                                @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve usage status for {} on {}", subjectName, volumeName);
        return retrieveQuotaForVolumeNameAndSubject(volumeName, subjectName);
    }

    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/report")
    public Response getQuotaReportForVolumeName(@PathParam("volumeName") String volumeName,
                                                @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve quota(s) for {} on {}", subjectName, volumeName);
        return retrieveQuotaForVolumeNameAndSubject(volumeName, subjectName);
    }

    private Response retrieveQuotaForVolumeNameAndSubject(String volumeName, String subjectName) {
        List<UsageData> usageData;
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

    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("quota/{volumeName}/report/{subjectName}")
    public Response getQuotaReportForVolumeNameAndSubject(@PathParam("volumeName") String volumeName,
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

    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("volume_quota/{storageVolumeId}/report")
    public Response getQuotaReportForVolumeId(@PathParam("storageVolumeId") Long storageVolumeId,
                                              @QueryParam("subjectName") String subjectName) {
        LOG.info("Retrieve user quota for {} on {}", subjectName, storageVolumeId);
        List<UsageData> usageData;
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

    @ApiOperation(value = "Retrieve a user's quota on a the specified storage volume.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid volume identifier or bad subject name for which no quota entry could be found"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("path_quota/{dataPath:.+}/report")
    public Response getQuotaReportForDataPath(@PathParam("dataPath") String dataPath,
                                              @QueryParam("subjectName") String subjectName) {
        String fullDataPath = StringUtils.prependIfMissing(dataPath, "/");
        LOG.info("Retrieve user quota for {} on {}", subjectName, fullDataPath);
        List<UsageData> usageData;
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
