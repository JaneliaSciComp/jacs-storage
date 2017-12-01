package org.janelia.jacsstorage.webdav;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.rest.AgentStorageResource;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.Propfind;
import org.janelia.jacsstorage.webdav.utils.WebdavUtils;

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
import java.util.List;

@Path(Constants.AGENTSTORAGE_URI_PATH)
public class AgentWebdavResource {

    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject
    private DataStorageService dataStorageService;

    @Context
    private UriInfo resourceURI;

    @LogStorageEvent(
            eventName = "DATASTORAGE_PROPFIND",
            argList = {0, 1, 2, 3}
    )
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("dataStorage/{dataBundleId}")
    public Response dataStoragePropFind(@PathParam("dataBundleId") Long dataBundleId,
                                        @HeaderParam("Depth") String depth,
                                        Propfind propfindRequest,
                                        @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        int depthValue = WebdavUtils.getDepth(depth);
        List<DataNodeInfo> dataBundleTree = dataStorageService.listDataEntries(dataBundle.getRealStoragePath(), dataBundle.getStorageFormat(), depthValue);
        Multistatus propfindResponse = WebdavUtils.convertNodeList(dataBundleTree, (nodeInfo) -> {
            String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                    ?  StringUtils.appendIfMissing(nodeInfo.getNodePath(), "/")
                    : nodeInfo.getNodePath();
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

    @LogStorageEvent(
            eventName = "DATASTORAGE_MKCOL",
            argList = {0, 1, 2}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @MKCOL
    @Path("dataStorage/{dataBundleId}/{dataDirPath: .+}")
    public Response createDataStorageDir(@PathParam("dataBundleId") Long dataBundleId,
                                         @PathParam("dataDirPath") String dataDirPath,
                                         @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        long dirEntrySize = dataStorageService.createDirectoryEntry(dataBundle.getRealStoragePath(), dataDirPath, dataBundle.getStorageFormat());
        long newBundleSize = dataBundle.size() + dirEntrySize;
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newBundleSize)
                        .build());
        return Response
                .created(resourceURI.getBaseUriBuilder().path(Constants.AGENTSTORAGE_URI_PATH).path("{dataBundleId}").build(dataBundleId))
                .build();
    }

}
