package org.janelia.jacsstorage.webdav;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.rest.AgentStorageResource;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.Propfind;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;
import org.janelia.jacsstorage.webdav.utils.WebdavUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Timed
@RequireAuthentication
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class AgentWebdavResource {

    private static final Logger LOG = LoggerFactory.getLogger(AgentWebdavResource.class);
    private static final long MAX_NODE_ENTRIES = 100000L;

    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageVolumeManager storageVolumeManager;
    @Inject
    private DataStorageService dataStorageService;

    @Context
    private UriInfo resourceURI;

    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @PROPFIND
    @Path("data_storage/{dataBundleId}{entry:(/entry/[^/]+?)?}")
    public Response dataStoragePropFindByStorageId(@PathParam("dataBundleId") Long dataBundleId,
                                                   @PathParam("entry") String entry,
                                                   @HeaderParam("Depth") String depth,
                                                   Propfind propfindRequest,
                                                   @Context SecurityContext securityContext) {
        LOG.info("PROPFIND data storage by ID: {}, Entry: {}, Depth: {} for {}", dataBundleId, entry, depth, securityContext.getUserPrincipal());
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        String entryName = StringUtils.isNotBlank(entry)
                ? entry.substring("/entry/".length())
                : null;
        int depthValue = WebdavUtils.getDepth(depth);
        Stream<DataNodeInfo> dataBundleNodesStream = dataStorageService.streamDataEntries(dataBundle.getRealStoragePath(), entryName, dataBundle.getStorageFormat(), depthValue).limit(MAX_NODE_ENTRIES);
        Multistatus propfindResponse = WebdavUtils.convertNodeList(dataBundleNodesStream,
                (nodeInfo) -> {
                    String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                            ? StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                            : nodeInfo.getNodeRelativePath();
                    nodeInfo.setNumericStorageId(dataBundle.getId());
                    nodeInfo.setStorageRootLocation(dataBundle.getRealStoragePath().toString());
                    nodeInfo.setStorageRootPathURI(dataBundle.getStorageURI());
                    nodeInfo.setNodeAccessURL(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_content")
                            .path(nodeInfoRelPath)
                            .build()
                            .toString()
                    );
                    nodeInfo.setNodeInfoURL(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path(dataBundle.getId().toString())
                            .path("data_info")
                            .path(nodeInfoRelPath)
                            .build()
                            .toString()
                    );
                    return nodeInfo;
                },
                DataNodeInfo::getNodeAccessURL);
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @PROPFIND
    @Path("data_storage_path/{dataPath:.+}")
    public Response dataStoragePropFindByPath(@PathParam("dataPath") String dataPathParam,
                                              @HeaderParam("Depth") String depthParam,
                                              Propfind propfindRequest,
                                              @Context SecurityContext securityContext) {
        LOG.debug("PROPFIND data storage by path: {}, Depth: {} for {}", dataPathParam, depthParam, securityContext.getUserPrincipal());
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(dataStorageService, storageLookupService, storageVolumeManager);
        int depth = WebdavUtils.getDepth(depthParam);
        Supplier<Response.ResponseBuilder> storageNotFoundHandler = () -> {
            // no storage volume was found
            LOG.warn("No storage volume found for {}", dataPathParam);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for " + dataPathParam);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    ;
        };
        return storageResourceHelper.handleResponseForFullDataPathParam(
                StoragePathURI.createAbsolutePathURI(dataPathParam),
                (dataBundle, dataEntryName) -> {
                    Stream<DataNodeInfo> dataBundleTree = dataStorageService.streamDataEntries(dataBundle.getRealStoragePath(), dataEntryName, dataBundle.getStorageFormat(), depth);
                    Multistatus multistatusResponse = WebdavUtils.convertNodeList(dataBundleTree,
                            (nodeInfo) -> {
                                String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                                        ? StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                                        : nodeInfo.getNodeRelativePath();
                                nodeInfo.setNumericStorageId(dataBundle.getId());
                                nodeInfo.setStorageRootLocation(dataBundle.getRealStoragePath().toString());
                                nodeInfo.setStorageRootPathURI(dataBundle.getStorageURI());
                                nodeInfo.setNodeAccessURL(resourceURI.getBaseUriBuilder()
                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                        .path(dataBundle.getId().toString())
                                        .path("data_content")
                                        .path(nodeInfoRelPath)
                                        .build()
                                        .toString()
                                );
                                nodeInfo.setNodeInfoURL(resourceURI.getBaseUriBuilder()
                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                        .path(dataBundle.getId().toString())
                                        .path("data_info")
                                        .path(nodeInfoRelPath)
                                        .build()
                                        .toString()
                                );
                                return nodeInfo;
                            },
                            DataNodeInfo::getNodeAccessURL);
                    if (multistatusResponse.getResponse().isEmpty()) {
                        LOG.warn("No path found for {}", dataPathParam);
                        Multistatus statusResponse = new Multistatus();
                        Propstat propstat = new Propstat();
                        propstat.setStatus("HTTP/1.1 404 Not Found");

                        PropfindResponse propfindResponse = new PropfindResponse();
                        propfindResponse.setResponseDescription("No path found for " + dataPathParam);
                        propfindResponse.setPropstat(propstat);
                        statusResponse.getResponse().add(propfindResponse);
                        return Response
                                .status(Response.Status.NOT_FOUND)
                                .entity(statusResponse)
                                ;
                    } else {
                        return Response.status(207)
                                .entity(multistatusResponse)
                                ;
                    }
                },
                (storageVolume, storageDataPathURI) -> {
                    java.nio.file.Path dataEntryPath = Paths.get(storageDataPathURI.getStoragePath());
                    JacsStorageFormat storageFormat = Files.isRegularFile(dataEntryPath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
                    Stream<DataNodeInfo> dataBundleNodesStream = dataStorageService.streamDataEntries(dataEntryPath, null, storageFormat, depth).limit(MAX_NODE_ENTRIES);
                    Multistatus multistatusResponse = WebdavUtils.convertNodeList(dataBundleNodesStream,
                            (nodeInfo) -> {
                                nodeInfo.setStorageRootLocation(storageVolume.getBaseStorageRootDir());
                                nodeInfo.setStorageRootPathURI(StoragePathURI.createPathURI(dataEntryPath.toString()));
                                return nodeInfo;
                            },
                            (nodeInfo) -> {
                                String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                                        ? StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                                        : nodeInfo.getNodeRelativePath();
                                return resourceURI.getBaseUriBuilder()
                                        .path(Constants.AGENTSTORAGE_URI_PATH)
                                        .path("storage_path")
                                        .path("data_content")
                                        .path(nodeInfoRelPath)
                                        .build()
                                        .toString();
                            });
                    if (multistatusResponse.getResponse().isEmpty()) {
                        LOG.warn("No path found for {}", dataPathParam);
                        Multistatus statusResponse = new Multistatus();
                        Propstat propstat = new Propstat();
                        propstat.setStatus("HTTP/1.1 404 Not Found");

                        PropfindResponse propfindResponse = new PropfindResponse();
                        propfindResponse.setResponseDescription("No path found for " + dataPathParam);
                        propfindResponse.setPropstat(propstat);
                        statusResponse.getResponse().add(propfindResponse);
                        return Response
                                .status(Response.Status.NOT_FOUND)
                                .entity(statusResponse)
                                ;
                    } else {
                        return Response.status(207)
                                .entity(multistatusResponse)
                                ;
                    }
                },
                storageNotFoundHandler
        ).build();
    }

    @LogStorageEvent(
            eventName = "DATASTORAGE_MKCOL",
            argList = {0, 1, 2}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @MKCOL
    @Path("data_storage/{dataBundleId}/{dataDirPath: .+}")
    public Response createDataStorageDir(@PathParam("dataBundleId") Long dataBundleId,
                                         @PathParam("dataDirPath") String dataDirPath,
                                         @Context SecurityContext securityContext) {
        LOG.debug("MKCOL {} : {} for {}", dataBundleId, dataDirPath, securityContext.getUserPrincipal());
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        long dirEntrySize = dataStorageService.createDirectoryEntry(dataBundle.getRealStoragePath(), dataDirPath, dataBundle.getStorageFormat());
        long newBundleSize = dataBundle.size() + dirEntrySize;
        storageAllocatorService.updateStorage(
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newBundleSize)
                        .build(),
                SecurityUtils.getUserPrincipal(securityContext)
        );
        return Response
                .created(resourceURI.getBaseUriBuilder().path(Constants.AGENTSTORAGE_URI_PATH).path("{dataBundleId}").build(dataBundleId))
                .build();
    }

}
