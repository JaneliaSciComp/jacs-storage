package org.janelia.jacsstorage.rest;

import com.google.common.base.Preconditions;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.io.TransferInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.security.JacsSubjectHelper;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
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
@Api(value = "Agent storage API. This API requires an authenticated subject.")
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
    @ApiOperation(value = "Persist the input stream in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Return the storage metadata"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 500, message = "Persistence error")
    })
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
    @ApiOperation(value = "Stream the entire data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 500, message = "Data read error")
    })
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
                .header("content-disposition","attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName())
                .build();
    }

    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("{dataBundleId}/list{entry:(/entry/[^/]+?)?}")
    @ApiOperation(value = "List the data bundle content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully read the data bundle content."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 500, message = "Data read error")
    })
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
        List<DataNodeInfo> dataBundleCotent = listDataEntries(dataBundle, entryName, depth);
        return Response
                .ok(dataBundleCotent, MediaType.APPLICATION_JSON)
                .header("content-disposition","attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName())
                .build();
    }

    private List<DataNodeInfo> listDataEntries(JacsBundle dataBundle, String entryName, int depth) {
        List<DataNodeInfo> dataBundleContent = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), entryName, dataBundle.getStorageFormat(), depth);
        if (CollectionUtils.isNotEmpty(dataBundleContent) && dataBundle.getVirtualRoot() != null) {
            String virtualStoragePath = dataBundle.getVirtualRoot();
            dataBundleContent.forEach(dn -> {
                dn.setStorageId(dataBundle.getId());
                dn.setRootPrefix(virtualStoragePath);
            });
        }
        return dataBundleContent;
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("{dataBundleId}/entry_content/{dataEntryPath:.*}")
    @ApiOperation(value = "Retrieve the content of the specified data bundle entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully read the data bundle entry's content."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response getEntryContent(@PathParam("dataBundleId") Long dataBundleId,
                                    @PathParam("dataEntryPath") String dataEntryPath,
                                    @Context SecurityContext securityContext) {
        LOG.info("Get entry {} content from bundle {} ", dataEntryPath, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.retrieveContentFromDataBundle(dataBundle, dataEntryPath);
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FOLDER",
            argList = {0, 1}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @POST
    @Path("{dataBundleId}/directory/{dataEntryPath:.*}")
    @ApiOperation(value = "Create a new folder in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "The new folder was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 409, message = "A folder with this name already exists"),
            @ApiResponse(code = 500, message = "Data write error")
    })
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
    @ApiOperation(value = "Create a new folder in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "The new folder was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 409, message = "A folder with this name already exists"),
            @ApiResponse(code = 500, message = "Data write error")
    })
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
        List<DataNodeInfo> existingEntries = listDataEntries(dataBundle, dataEntryPath, 0);
        if (CollectionUtils.isNotEmpty(existingEntries)) {
            return Response
                    .status(Response.Status.CONFLICT)
                    .location(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundleId.toString())
                            .path("entry_content")
                            .path(dataEntryPath)
                            .build())
                    .entity(existingEntries.get(0))
                    .build();
        }

        long newDirEntrySize = dataStorageService.createDirectoryEntry(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat());
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newDirEntrySize)
                        .build());
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setRootLocation(dataBundle.getRealStoragePath().toString());
        newDataNode.setRootPrefix(dataBundle.getVirtualRoot());
        newDataNode.setNodeRelativePath(dataEntryPath);
        newDataNode.setCollectionFlag(true);
        return Response
                .created(resourceURI.getBaseUriBuilder()
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path(dataBundleId.toString())
                        .path("entry_content")
                        .path(dataEntryPath)
                        .build())
                .entity(newDataNode)
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
    @ApiOperation(value = "Create a new content entry in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "The new content was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 409, message = "A file with this name already exists"),
            @ApiResponse(code = 500, message = "Data write error")
    })
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
    @ApiOperation(value = "Create a new content entry in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "The new content was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 409, message = "A file with this name already exists"),
            @ApiResponse(code = 500, message = "Data write error")
    })
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
        List<DataNodeInfo> existingEntries = listDataEntries(dataBundle, dataEntryPath, 0);
        if (CollectionUtils.isNotEmpty(existingEntries)) {
            return Response
                    .status(Response.Status.CONFLICT)
                    .location(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundleId.toString())
                            .path("entry_content")
                            .path(dataEntryPath)
                            .build())
                    .entity(existingEntries.get(0))
                    .build();
        }
        long newFileEntrySize = dataStorageService.createFileEntry(dataBundle.getRealStoragePath(), dataEntryPath, dataBundle.getStorageFormat(), contentStream);
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newFileEntrySize)
                        .build());
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setRootLocation(dataBundle.getRealStoragePath().toString());
        newDataNode.setRootPrefix(dataBundle.getVirtualRoot());
        newDataNode.setNodeRelativePath(dataEntryPath);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(resourceURI.getBaseUriBuilder()
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path(dataBundleId.toString())
                        .path("entry_content")
                        .path(dataEntryPath)
                        .build())
                .entity(newDataNode)
                .build();
    }

    @LogStorageEvent(
            eventName = "DELETE_STORAGE",
            argList = {0, 1}
    )
    @DELETE
    @Path("{dataBundleId}")
    @ApiOperation(value = "Delete the entire specified data bundle. Use this operation with caution because at this point there's no backup and data cannot be restored")
    @ApiResponses(value = {
            @ApiResponse(code = 202, message = "The new content was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 409, message = "A file with this name already exists"),
            @ApiResponse(code = 500, message = "Data write error")
    })
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
