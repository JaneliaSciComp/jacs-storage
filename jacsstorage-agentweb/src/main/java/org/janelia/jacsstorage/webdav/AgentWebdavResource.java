package org.janelia.jacsstorage.webdav;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.Timed;
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

@Timed
@RequireAuthentication
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class AgentWebdavResource {

    private static final Logger LOG = LoggerFactory.getLogger(AgentWebdavResource.class);

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
    @Produces(MediaType.APPLICATION_XML)
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
        List<DataNodeInfo> dataBundleTree = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), entryName, dataBundle.getStorageFormat(), depthValue);
        Multistatus propfindResponse = WebdavUtils.convertNodeList(dataBundleTree, (nodeInfo) -> {
            String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                    ?  StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                    : nodeInfo.getNodeRelativePath();
            return resourceURI.getBaseUriBuilder()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path(dataBundleId.toString())
                    .path(nodeInfoRelPath)
                    .build()
                    .toString();
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("data_storage_path/{dataStoragePath:.+}")
    public Response dataStoragePropFindByPath(@PathParam("dataStoragePath") String dataStoragePath,
                                              @HeaderParam("Depth") String depth,
                                              Propfind propfindRequest,
                                              @Context SecurityContext securityContext) {
        LOG.info("PROPFIND data storage by path: {}, Depth: {} for {}", dataStoragePath, depth, securityContext.getUserPrincipal());
        String fullDataStoragePathName = StringUtils.prependIfMissing(dataStoragePath, "/");
        List<JacsStorageVolume> localVolumes = storageVolumeManager.getManagedVolumes(new StorageQuery().setDataStoragePath(fullDataStoragePathName));
        if (localVolumes.isEmpty()) {
            LOG.warn("No storage volume found for {}", dataStoragePath);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for " + fullDataStoragePathName);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        } else if (localVolumes.size() > 1) {
            LOG.warn("More than one storage volumes found for {} -> {}", dataStoragePath, localVolumes);
        }
        JacsStorageVolume storageVolume = localVolumes.get(0); // even if there are more volumes pick the first one - this assumes that the first one has the longest match
        int depthValue = WebdavUtils.getDepth(depth);
        java.nio.file.Path storageRelativeFileDataPath = storageVolume.getStorageRelativePath(fullDataStoragePathName);
        int fileDataPathComponents = storageRelativeFileDataPath.getNameCount();
        JacsBundle dataBundle = null;
        try {
            if (fileDataPathComponents > 0) {
                String bundleIdComponent = storageRelativeFileDataPath.getName(0).toString();
                Number bundleId = new BigInteger(bundleIdComponent);
                dataBundle = storageLookupService.getDataBundleById(bundleId);
            }
        } catch (NumberFormatException e) {
            LOG.debug("Path {} is not a data bundle - first component is not numeric", storageRelativeFileDataPath);
        }
        if (dataBundle == null) {
            if (Files.exists(Paths.get(storageVolume.getStorageRootDir()).resolve(storageRelativeFileDataPath))) {
                return listStorageEntriesForPath(Paths.get(storageVolume.getStorageRootDir()).resolve(storageRelativeFileDataPath), depthValue);
            } else if (Files.exists(Paths.get(storageVolume.getStoragePathPrefix()).resolve(storageRelativeFileDataPath))) {
                return listStorageEntriesForPath(Paths.get(storageVolume.getStoragePathPrefix()).resolve(storageRelativeFileDataPath), depthValue);
            } else {
                LOG.warn("No path found for {} relative to {}", fullDataStoragePathName, storageVolume);
                return Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(new ErrorResponse("No path found for " + fullDataStoragePathName))
                        .build();
            }
        } else {
            String dataEntryPath = fileDataPathComponents > 1
                    ? storageRelativeFileDataPath.subpath(1, fileDataPathComponents).toString()
                    : "";
            return listStorageEntriesForBundle(dataBundle, dataEntryPath, depthValue);
        }
    }

    private Response listStorageEntriesForPath(java.nio.file.Path storagePath, int depth) {
        JacsStorageFormat storageFormat = Files.isRegularFile(storagePath) ? JacsStorageFormat.SINGLE_DATA_FILE : JacsStorageFormat.DATA_DIRECTORY;
        List<DataNodeInfo> dataBundleTree = dataStorageService.listDataEntries(storagePath, null, storageFormat, depth);
        Multistatus propfindResponse = WebdavUtils.convertNodeList(dataBundleTree, (nodeInfo) -> {
            String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                    ?  StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                    : nodeInfo.getNodeRelativePath();
            return resourceURI.getBaseUriBuilder()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path("storage_path")
                    .path(nodeInfoRelPath)
                    .build()
                    .toString();
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    private Response listStorageEntriesForBundle(JacsBundle storageBundle, String dataEntryPath, int depth) {
        List<DataNodeInfo> dataBundleTree = dataStorageService.listDataEntries(storageBundle.getRealStoragePath(), dataEntryPath, storageBundle.getStorageFormat(), depth);
        Multistatus propfindResponse = WebdavUtils.convertNodeList(dataBundleTree, (nodeInfo) -> {
            String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                    ?  StringUtils.appendIfMissing(nodeInfo.getNodeRelativePath(), "/")
                    : nodeInfo.getNodeRelativePath();
            return resourceURI.getBaseUriBuilder()
                    .path(Constants.AGENTSTORAGE_URI_PATH)
                    .path(storageBundle.getId().toString())
                    .path("entry_content")
                    .path(nodeInfoRelPath)
                    .build()
                    .toString();
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
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
        LOG.info("MKCOL {} : {}", dataBundleId, dataDirPath);
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
