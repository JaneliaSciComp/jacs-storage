package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

@RequestScoped
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class AgentStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(AgentStorageResource.class);
    private static int MAX_ALLOWED_DEPTH = 20;

    @Inject
    private DataStorageService dataStorageService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;

    @Context
    private UriInfo resourceURI;

    @LogStorageEvent(
            eventName = "HTTP_STREAM_STORAGE_DATA",
            argList = {0, 1}
    )
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @POST
    @Path("{dataBundleId}")
    public Response persistStream(@PathParam("dataBundleId") Long dataBundleId,
                                  @Context SecurityContext securityContext,
                                  InputStream bundleStream) throws IOException {
        LOG.info("Create data storage bundle for {}", dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        TransferInfo ti = dataStorageService.persistDataStream(dataBundle.getRealStoragePath(), dataBundle.getStorageFormat(), bundleStream);
        dataBundle.setChecksum(Base64.getEncoder().encodeToString(ti.getChecksum()));
        dataBundle.setUsedSpaceInBytes(ti.getNumBytes());
        storageAllocatorService.updateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
        return Response
                .ok(DataStorageInfo.fromBundle(dataBundle))
                .build();
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("{dataBundleId}")
    public Response retrieveStream(@PathParam("dataBundleId") Long dataBundleId,
                                   @Context SecurityContext securityContext) {
        LOG.info("Retrieve the entire stored bundle {}", dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        StreamingOutput bundleStream = output -> {
            try {
                dataStorageService.retrieveDataStream(dataBundle.getRealStoragePath(), dataBundle.getStorageFormat(), output);
                output.flush();
            } catch (Exception e) {
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName())
                .build();
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("path/{filePath:.+}")
    public Response retrieveFile(@PathParam("filePath") String fullFileNameParam,
                                 @Context SecurityContext securityContext) {
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageLookupService, storageVolumeManager);
        return storageResourceHelper.retrieveFileContent(
                fullFileNameParam,
                () -> Response
                        .status(Response.Status.CONFLICT)
                        .entity(new ErrorResponse("More than one volume found for " + fullFileNameParam))
                        .build(),
                (dataBundle, dataEntryPath) -> retrieveFileFromBundle(dataBundle, dataEntryPath),
                (storageVolume, dataEntryPath) -> {
                    if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath))) {
                        return retrieveFileUsingFilePath(Paths.get(storageVolume.getStorageRootDir()).resolve(dataEntryPath));
                    } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath))) {
                        return retrieveFileUsingFilePath(Paths.get(storageVolume.getStoragePathPrefix()).resolve(dataEntryPath));
                    } else {
                        return Response
                                .status(Response.Status.NOT_FOUND)
                                .entity(new ErrorResponse("No path found for " + fullFileNameParam))
                                .build();
                    }
                }
        );
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storageVolume/{storageVolumeId}/{storageRelativePath:.+}")
    public Response retrieveFileFromStorageVolume(@PathParam("storageVolumeId") Long storageVolumeId,
                                                  @PathParam("storageRelativePath") String storageRelativeFilePath) {
        JacsStorageVolume storageVolume = storageVolumeManager.getVolumeById(storageVolumeId);
        if (storageVolume == null) {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No managed volume found for " + storageVolumeId))
                    .build();
        }
        if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(storageRelativeFilePath))) {
            return retrieveFileUsingFilePath(Paths.get(storageVolume.getStorageRootDir()).resolve(storageRelativeFilePath));
        } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(storageRelativeFilePath))) {
            return retrieveFileUsingFilePath(Paths.get(storageVolume.getStoragePathPrefix()).resolve(storageRelativeFilePath));
        } else {
            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(new ErrorResponse("No path found for " + storageRelativeFilePath + " on " + storageVolumeId))
                    .build();
        }
    }

    private Response retrieveFileUsingFilePath(java.nio.file.Path filePath) {
        JacsStorageFormat storageFormat = Files.isRegularFile(filePath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        StreamingOutput fileStream = output -> {
            try {
                dataStorageService.retrieveDataStream(filePath, storageFormat, output);
                output.flush();
            } catch (Exception e) {
                LOG.error("Error streaming data file content for {}", filePath, e);
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(fileStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + filePath.toFile().getName())
                .build();
    }

    private Response retrieveFileFromBundle(JacsBundle dataBundle, String dataEntryPath) {
        StreamingOutput bundleStream = output -> {
            try {
                dataStorageService.readDataEntryStream(
                        dataBundle.getRealStoragePath(),
                        dataEntryPath,
                        dataBundle.getStorageFormat(),
                        output);
                output.flush();
            } catch (Exception e) {
                LOG.error("Error streaming data file content for {}:{}", dataBundle, dataEntryPath, e);
                throw new WebApplicationException(e);
            }
        };
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName() + "/" + dataEntryPath)
                .build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("{dataBundleId}/list{entry:(/entry/[^/]+?)?}")
    public Response listContent(@PathParam("dataBundleId") Long dataBundleId,
                                @PathParam("entry") String entry,
                                @QueryParam("depth") Integer depthParam,
                                @Context SecurityContext securityContext) {
        LOG.info("List bundle content {} with a depthParameter {}", dataBundleId, depthParam);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        String entryName = StringUtils.isNotBlank(entry)
                ? entry.substring("/entry/".length())
                : null;
        int depth = depthParam != null && depthParam >= 0 && depthParam < MAX_ALLOWED_DEPTH ? depthParam : MAX_ALLOWED_DEPTH;
        List<DataNodeInfo> dataBundleCotent = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), entryName, dataBundle.getStorageFormat(), depth);
        return Response
                .ok(dataBundleCotent, MediaType.APPLICATION_JSON)
                .header("content-disposition","attachment; filename = " + dataBundle.getOwner() + "-" + dataBundle.getName())
                .build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("{dataBundleId}/entry-content/{dataEntryPath:.*}")
    public Response getEntryContent(@PathParam("dataBundleId") Long dataBundleId,
                                    @PathParam("dataEntryPath") String dataEntryPath,
                                    @Context SecurityContext securityContext) {
        LOG.info("Get entry {} content from bundle {} ", dataEntryPath, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        return retrieveFileFromBundle(dataBundle, dataEntryPath);
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FOLDER",
            argList = {0, 1}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @POST
    @Path("{dataBundleId}/directory/{dataEntryPath:.*}")
    public Response postCreateDirectory(@PathParam("dataBundleId") Long dataBundleId,
                                        @PathParam("dataEntryPath") String dataEntryPath,
                                        @Context SecurityContext securityContext) {
        return createDirectory(dataBundleId, dataEntryPath, securityContext);
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FOLDER",
            argList = {0, 1, 2}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @PUT
    @Path("{dataBundleId}/directory/{dataEntryPath:.*}")
    public Response putCreateDirectory(@PathParam("dataBundleId") Long dataBundleId,
                                       @PathParam("dataEntryPath") String dataEntryPath,
                                       @Context SecurityContext securityContext) {
        return createDirectory(dataBundleId, dataEntryPath, securityContext);
    }

    private Response createDirectory(Long dataBundleId,
                                    String dataEntryPath,
                                    SecurityContext securityContext) {
        LOG.info("Create new directory {} under {} ", dataEntryPath, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        long newDirEntrySize = dataStorageService.createDirectoryEntry(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat());
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newDirEntrySize)
                        .build());
        return Response
                .created(resourceURI.getBaseUriBuilder()
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path(dataBundleId.toString())
                        .path("entry-content")
                        .path(dataEntryPath)
                        .build())
                .build();
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1, 2}
    )
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @POST
    @Path("{dataBundleId}/file/{dataEntryPath:.*}")
    public Response postCreateFile(@PathParam("dataBundleId") Long dataBundleId,
                                   @PathParam("dataEntryPath") String dataEntryPath,
                                   @Context SecurityContext securityContext,
                                   InputStream contentStream) {
        return createFile(dataBundleId, dataEntryPath, securityContext, contentStream);
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1, 2}
    )
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @PUT
    @Path("{dataBundleId}/file/{dataEntryPath:.*}")
    public Response putCreateFile(@PathParam("dataBundleId") Long dataBundleId,
                                  @PathParam("dataEntryPath") String dataEntryPath,
                                  @Context SecurityContext securityContext,
                                  InputStream contentStream) {
        return createFile(dataBundleId, dataEntryPath, securityContext, contentStream);
    }

    private Response createFile(Long dataBundleId,
                                String dataEntryPath,
                                SecurityContext securityContext,
                                InputStream contentStream) {
        LOG.info("Create new file {} under {} ", dataEntryPath, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        List<DataNodeInfo> existingEntries = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), 0);
        if (CollectionUtils.isNotEmpty(existingEntries)) {
            return Response
                    .status(Response.Status.CONFLICT)
                    .location(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundleId.toString())
                            .path("entry-content")
                            .path(dataEntryPath)
                            .build())
                    .build();
        }
        long newFileEntrySize = dataStorageService.createFileEntry(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), contentStream);
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newFileEntrySize)
                        .build());
        return Response
                .created(resourceURI.getBaseUriBuilder()
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path(dataBundleId.toString())
                        .path("entry-content")
                        .path(dataEntryPath)
                        .build())
                .build();
    }

    @LogStorageEvent(
            eventName = "DELETE_STORAGE",
            argList = {0, 1}
    )
    @DELETE
    @Path("{dataBundleId}")
    public Response deleteStorage(@PathParam("dataBundleId") Long dataBundleId,
                                  @Context SecurityContext securityContext) throws IOException {
        LOG.info("Delete bundle {}", dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        if (dataBundle != null) {
            dataStorageService.deleteStorage(dataBundle.getRealStoragePath());
            dataStorageService.cleanupStoragePath(dataBundle.getRealStoragePath().getParent());
        }
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
