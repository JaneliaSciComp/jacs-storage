package org.janelia.jacsstorage.rest;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HEAD;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Request;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.StreamingOutput;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.Provider;

import com.google.common.base.Preconditions;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.support.JacsSubjectHelper;
import org.janelia.jacsstorage.requesthelpers.ContentAccessRequestHelper;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag(name = "DataBundleStorage",
     description = "Agent storage API. This API requires an authenticated subject.")
@Timed
@RequireAuthentication
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class DataBundleStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(DataBundleStorageResource.class);

    @Inject
    private DataContentService dataContentService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;

    @Operation(
            summary = "List the data bundle content.",
            description = "Lists tree hierarchy of the storage bundle - if entry is specified it only lists the specified entry sub-tree"
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully read the data bundle content."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{dataBundleId}/list")
    public Response listContent(@PathParam("dataBundleId") Long dataBundleId,
                                @QueryParam("entry") String entry,
                                @QueryParam("depth") Integer depthParam,
                                @QueryParam("offset") Integer offsetParam,
                                @QueryParam("offset") Integer lengthParam,
                                @Context UriInfo requestURI,
                                @Context SecurityContext securityContext) {
        LOG.info("List bundle content {} with a depthParameter {}", dataBundleId, depthParam);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        String entryName = StringUtils.isNotBlank(entry)
                ? entry.trim()
                : null;
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        int offset = offsetParam != null ? offsetParam : 0;
        int length = lengthParam != null ? lengthParam : -1;
        JADEStorageURI storageURI = dataBundle.getStorageURI().resolve(entryName);
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters())
                .setMaxDepth(depth)
                .setEntriesCount(length)
                .setStartEntryIndex(offset);

        ContentGetter contentGetter = dataContentService.getDataContent(storageURI, contentAccessParams);
        List<DataNodeInfo> contentNodes = contentGetter.getObjectsList().stream()
                .map(contentNode -> {
                    DataNodeInfo dn = new DataNodeInfo();
                    dn.setNumericStorageId(dataBundle.getId());
                    dn.setSize(contentNode.getSize());
                    dn.setMimeType(contentNode.getMimeType());
                    dn.setLastModified(contentNode.getLastModified());
                    dn.setStorageType(contentNode.getStorageType().name());
                    dn.setStorageRootLocation(dataBundle.getStorageURI().getJadeStorage());
                    dn.setStorageRootBinding(dataBundle.getStorageRootBinding());
                    dn.setCollectionFlag(contentNode.isCollection());
                    dn.setNodeAccessURL(requestURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_content")
                            .path(dn.getNodeRelativePath())
                            .build()
                            .toString()
                    );
                    dn.setNodeInfoURL(requestURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_info")
                            .path(dn.getNodeRelativePath())
                            .build()
                            .toString()
                    );
                    return dn;
                })
                .collect(Collectors.toList());
        return Response
                .ok(contentNodes, MediaType.APPLICATION_JSON)
                .build();
    }

    @Operation(
            summary = "Retrieve the content of the specified data bundle entry.",
            description = "Retrieve the specified entry's content. If the entry is a directory entry it streams the entire subdirectory tree as a tar archive, " +
                          "otherwise if it is a file it streams the specified file's content."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully read the data bundle entry's content."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @HEAD
    @Produces({MediaType.APPLICATION_JSON})
    @Path("{dataBundleId}/data_content{dataEntryPath:(/.*)?}")
    public Response checkEntryContent(@PathParam("dataBundleId") Long dataBundleId,
                                      @PathParam("dataEntryPath") String dataEntryPathParam,
                                      @QueryParam("directoryOnly") Boolean directoryOnlyParam,
                                      @Context UriInfo requestURI,
                                      @Context ContainerRequestContext requestContext,
                                      @Context SecurityContext securityContext) {
        LOG.info("Get entry {} content from bundle {} ", dataEntryPathParam, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        JADEStorageURI storageURI = dataBundle.getStorageURI().resolve(dataEntryPathParam);
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
        ContentGetter contentGetter = dataContentService.getDataContent(storageURI, contentAccessParams);
        List<ContentNode> contentNodes = contentGetter.getObjectsList();
        if (contentNodes.isEmpty()) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } else {
            return Response.ok().build();
        }
    }

    @Operation(
            summary = "Retrieve the content of the specified data bundle entry.",
            description = "Retrieve the specified entry's content. If the entry is a directory entry it streams the entire subdirectory tree as a tar archive, " +
                          "otherwise if it is a file it streams the specified file's content."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully read the data bundle entry's content."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_OCTET_STREAM})
    @Path("{dataBundleId}/data_content{dataEntryPath:(/.*)?}")
    public Response getEntryContent(@PathParam("dataBundleId") Long dataBundleId,
                                    @PathParam("dataEntryPath") String dataEntryPathParam,
                                    @Context UriInfo requestURI) {
        LOG.info("Get entry {} content from bundle {} ", dataEntryPathParam, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        JADEStorageURI storageURI = dataBundle.getStorageURI().resolve(dataEntryPathParam);
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
        ContentGetter contentGetter = dataContentService.getDataContent(storageURI, contentAccessParams);
        long contentSize = contentGetter.estimateContentSize();
        StreamingOutput bundleStream = output -> contentGetter.streamContent(output);
        return Response
                .ok(bundleStream, MediaType.APPLICATION_OCTET_STREAM)
                .header("Content-Length", contentSize)
                .header("content-disposition","attachment; filename = " + JacsSubjectHelper.getNameFromSubjectKey(dataBundle.getOwnerKey()) + "-" + dataBundle.getName())
                .build();
    }

    @Operation(
            summary = "Retrieve the metadata of the specified data bundle entry.",
            description = "Retrieve the specified entry's metadata."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully read the data bundle entry's metadata."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data read error")
    })
    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("{dataBundleId}/data_info{dataEntryPath:/?.*}")
    public Response getEntryContentInfo(@PathParam("dataBundleId") Long dataBundleId,
                                        @PathParam("dataEntryPath") String dataEntryPathParam,
                                        @Context UriInfo requestURI) {
        LOG.info("Get entry {} content from bundle {} ", dataEntryPathParam, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        JADEStorageURI storageURI = dataBundle.getStorageURI().resolve(dataEntryPathParam);
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters());
        ContentGetter contentGetter = dataContentService.getDataContent(storageURI, contentAccessParams);
        return Response.ok(contentGetter.getMetaData()).build();
    }

    @Operation(description = "Create a new content entry in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "The new content was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "409", description = "A file with this name already exists"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1, 2}
    )
    @TimedMethod(argList = {0, 1, 2})
    @POST
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{dataBundleId}/data_content{dataEntryPath:(/.*)?}")
    public Response postDataContent(@PathParam("dataBundleId") Long dataBundleId,
                                    @PathParam("dataEntryPath") String dataEntryPath,
                                    @Context SecurityContext securityContext,
                                    @Context UriInfo requestURI,
                                    InputStream contentStream) {
        return createDataContent(dataBundleId, dataEntryPath, securityContext, requestURI, contentStream);
    }

    @Operation(description = "Create a new content entry in the specified data bundle.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "201", description = "The new content was created successfully. Return the URL of the corresponding entry in the location header attribute."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "409", description = "A file with this name already exists"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1, 2}
    )
    @TimedMethod(argList = {0, 1, 2})
    @PUT
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{dataBundleId}/data_content{dataEntryPath:(/.*)?}")
    public Response putDataContent(@PathParam("dataBundleId") Long dataBundleId,
                                   @PathParam("dataEntryPath") String dataEntryPath,
                                   @Context SecurityContext securityContext,
                                   @Context UriInfo requestURI,
                                   InputStream contentStream) {
        return createDataContent(dataBundleId, dataEntryPath, securityContext, requestURI, contentStream);
    }

    private Response createDataContent(Long dataBundleId,
                                       String dataEntryPathParam,
                                       SecurityContext securityContext,
                                       UriInfo requestURI,
                                       InputStream contentStream) {
        LOG.info("Create new file {} under {} ", dataEntryPathParam, dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        String dataEntryPath = StringUtils.removeStart(dataEntryPathParam, "/");
        URI dataNodeAccessURI = requestURI.getBaseUriBuilder()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundleId.toString())
                .path("data_content")
                .path(dataEntryPath)
                .build()
                ;
        // not handling any conflict - the new entry will override an existing one
        JADEStorageURI dataEntryStorageURI = dataBundle.getStorageURI().resolve(dataEntryPathParam);
        long newFileEntrySize = dataContentService.writeDataStream(dataEntryStorageURI, contentStream);
        storageAllocatorService.updateStorage(
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newFileEntrySize)
                        .build(),
                SecurityUtils.getUserPrincipal(securityContext)
        );
        DataNodeInfo newDataNode = new DataNodeInfo();
        newDataNode.setNumericStorageId(dataBundleId);
        newDataNode.setStorageType(dataBundle.getStorageURI().getStorageType().name());
        newDataNode.setStorageRootLocation(dataBundle.getStorageURI().getJadeStorage());
        newDataNode.setStorageRootBinding(dataBundle.getStorageRootBinding());
        newDataNode.setNodeAccessURL(dataNodeAccessURI.toString());
        newDataNode.setNodeInfoURL(requestURI.getBaseUriBuilder()
                .path(Constants.AGENTSTORAGE_URI_PATH)
                .path(dataBundleId.toString())
                .path("data_info")
                .path(dataEntryPath)
                .build()
                .toString()
        );
        newDataNode.setNodeRelativePath(dataEntryPath);
        newDataNode.setCollectionFlag(false);
        return Response
                .created(dataNodeAccessURI)
                .entity(newDataNode)
                .build();
    }

    @Operation(description = "Delete the entire specified data bundle. Use this operation with caution because at this point there's no backup and data cannot be restored")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "204", description = "The storage was deleted successfully."),
            @ApiResponse(responseCode = "404", description = "Invalid data bundle ID"),
            @ApiResponse(responseCode = "500", description = "Data write error")
    })
    @LogStorageEvent(
            eventName = "DELETE_STORAGE",
            argList = {0, 1}
    )
    @TimedMethod
    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("{dataBundleId}")
    public Response deleteStorage(@PathParam("dataBundleId") Long dataBundleId,
                                  @Context SecurityContext securityContext) throws IOException {
        LOG.info("Delete bundle {}", dataBundleId);
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        if (dataBundle != null) {
            JADEStorageURI storageURI = dataBundle.getStorageURI();
            dataContentService.removeData(storageURI);
            storageAllocatorService.deleteStorage(dataBundle, SecurityUtils.getUserPrincipal(securityContext));
        }
        return Response
                .status(Response.Status.NO_CONTENT)
                .build();
    }

}
