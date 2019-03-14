package org.janelia.jacsstorage.rest.nonauthenticated;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ContentFilterRequestHelper;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

@Api(value = "Agent storage API based on file's path.")
@Timed
@Produces(MediaType.APPLICATION_JSON)
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class PathBasedAgentStorageUnauthenticatedResource {

    private static final Logger LOG = LoggerFactory.getLogger(PathBasedAgentStorageUnauthenticatedResource.class);

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

}
