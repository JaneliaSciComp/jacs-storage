package org.janelia.jacsstorage.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.security.JacsCredentials;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.io.InputStream;
import java.net.URI;

@Api(value = "Agent storage API based on file's path.")
@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class PathBasedAgentStorageResource {

    private static final Logger LOG = LoggerFactory.getLogger(PathBasedAgentStorageResource.class);

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

    @Produces({MediaType.TEXT_PLAIN, MediaType.TEXT_HTML, MediaType.APPLICATION_JSON})
    @HEAD
    @Path("storage_path/data_content/{dataPath:.+}")
    @ApiOperation(value = "Check if the specified file path identifies a valid data bundle entry content.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The content was found"),
            @ApiResponse(code = 404, message = "Invalid file path"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response checkPath(@PathParam("dataPath") String dataPathParam, @QueryParam("directoryOnly") Boolean directoryOnlyParam) {
        LOG.info("Check path {}", dataPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryPath) -> storageResourceHelper.checkContentFromDataBundle(dataBundle, dataEntryPath, directoryOnlyParam != null && directoryOnlyParam),
                (storageVolume, dataEntryPath) -> storageResourceHelper.checkContentFromFile(storageVolume, dataEntryPath, directoryOnlyParam != null && directoryOnlyParam)
        ).build();
    }

    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_path/data_content/{dataPath:.+}")
    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveData(@PathParam("dataPath") String dataPathParam,
                                 @Context UriInfo requestURI) {
        LOG.info("Retrieve data from {}", dataPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        ContentFilterParams filterParams = ContentFilterRequestHelper.createContentFilterParamsFromQuery(requestURI.getQueryParameters());
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryPath) -> storageResourceHelper.retrieveContentFromDataBundle(dataBundle, filterParams, dataEntryPath),
                (storageVolume, dataEntryPath) -> storageResourceHelper.retrieveContentFromFile(storageVolume, filterParams, dataEntryPath)
        ).build();
    }

    @Deprecated
    @Produces({MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_path/{dataPath:.+}")
    @ApiOperation(value = "Deprecated endpoint to retrieve the content of the specified data path. Use 'storage_path/data_content/{dataPath:.+}' instead ")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The stream was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response deprecatedRetrieveData(@PathParam("dataPath") String dataPathParam,
                                           @Context UriInfo requestURI) {
        return retrieveData(dataPathParam, requestURI);
    }

    @LogStorageEvent(
            eventName = "DELETE_STORAGE_ENTRY",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Produces({MediaType.APPLICATION_JSON})
    @DELETE
    @Path("storage_path/data_content/{dataPath:.+}")
    @ApiOperation(value = "Retrieve the content of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The data streaming was successfull"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response removeData(@PathParam("dataPath") String dataPathParam, @Context SecurityContext securityContext) {
        LOG.info("Remove data from {}", dataPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        JacsCredentials credentials = SecurityUtils.getUserPrincipal(securityContext);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) -> storageResourceHelper.removeContentFromDataBundle(dataBundle,
                        dataEntryName,
                        credentials,
                        (Long freedEntrySize) -> storageAllocatorService.updateStorage(
                                new JacsBundleBuilder()
                                        .dataBundleId(dataBundle.getId())
                                        .usedSpaceInBytes(-freedEntrySize)
                                        .build(),
                                credentials)),
                (storageVolume, dataEntryName) -> storageResourceHelper.removeFileContentFromVolume(storageVolume, dataEntryName)
        ).build();
    }

    @LogStorageEvent(
            eventName = "CREATE_STORAGE_FILE",
            argList = {0, 1}
    )
    @RequireAuthentication
    @Produces({MediaType.APPLICATION_JSON})
    @PUT
    @Path("storage_path/data_content/{dataPath:.+}")
    @ApiOperation(value = "Store the content at the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "The data was saved successfully"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data write error")
    })
    public Response storeData(@PathParam("dataPath") String dataPathParam, @Context SecurityContext securityContext, InputStream contentStream) {
        LOG.info("Retrieve data from {}", dataPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) ->
                        storageResourceHelper.storeDataBundleContent(dataBundle,
                                resourceURI.getBaseUri(),
                                dataEntryName,
                                (Long newEntrySize) -> storageAllocatorService.updateStorage(
                                            new JacsBundleBuilder()
                                                    .dataBundleId(dataBundle.getId())
                                                    .usedSpaceInBytes(newEntrySize)
                                                    .build(),
                                            SecurityUtils.getUserPrincipal(securityContext)),
                                contentStream),
                (storageVolume, dataEntryName) ->
                        storageResourceHelper.storeFileContent(storageVolume,
                                resourceURI.getBaseUri(),
                                dataEntryName,
                                contentStream)
        ).build();
    }

    @RequireAuthentication
    @Produces({MediaType.APPLICATION_JSON})
    @GET
    @Path("storage_path/data_info/{dataPath:.+}")
    @ApiOperation(value = "Inspect and retrieve content info of the specified data path.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "The operation was successfull. " +
                    "As a note the operation is considered successful even if there's currently no process to extract any information from the content"),
            @ApiResponse(code = 404, message = "Invalid data bundle identifier"),
            @ApiResponse(code = 409, message = "This may be caused by a misconfiguration which results in the system not being able to identify the volumes that hold the data file"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response retrieveDataInfo(@PathParam("dataPath") String dataPathParam) {
        LOG.info("Retrieve data info from {}", dataPathParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryPath) -> storageResourceHelper.retrieveContentInfoFromDataBundle(dataBundle, dataEntryPath),
                (storageVolume, dataEntryPath) -> storageResourceHelper.retrieveContentInfoFromFile(storageVolume, dataEntryPath)
        ).build();
    }

    @RequireAuthentication
    @Produces(MediaType.APPLICATION_JSON)
    @GET
    @Path("storage_path/list/{dataPath:.*}")
    @ApiOperation(
            value = "List the content.",
            notes = "Lists tree hierarchy of the storage path"
    )
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully read the data bundle content."),
            @ApiResponse(code = 404, message = "Invalid data bundle ID"),
            @ApiResponse(code = 500, message = "Data read error")
    })
    public Response listContent(@PathParam("dataPath") String dataPathParam,
                                @QueryParam("depth") Integer depthParam,
                                @Context SecurityContext securityContext) {
        LOG.info("List content from location {} with a depthParameter {}", dataPathParam, depthParam);
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        int depth = depthParam != null && depthParam >= 0 && depthParam < Constants.MAX_ALLOWED_DEPTH ? depthParam : Constants.MAX_ALLOWED_DEPTH;
        URI baseURI = resourceURI.getBaseUri();
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) -> storageResourceHelper.listContentFromDataBundle(dataBundle, baseURI, dataEntryName, depth),
                (storageVolume, dataEntryName) -> storageResourceHelper.listContentFromPath(storageVolume, baseURI, dataEntryName, depth)
        ).build();
    }

}
