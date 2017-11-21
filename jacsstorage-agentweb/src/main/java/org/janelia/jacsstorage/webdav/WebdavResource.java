package org.janelia.jacsstorage.webdav;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.datarequest.DataStorageInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.rest.AgentStorageResource;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.Prop;
import org.janelia.jacsstorage.webdav.propfind.Propfind;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;

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
import java.util.Optional;
import java.util.stream.Collectors;

@Path("agent-webdav")
public class WebdavResource {

    private static int MAX_ALLOWED_DEPTH = 20;

    @Inject
    @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject @LocalInstance
    private StorageAllocatorService storageAllocatorService;
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
        int depthValue = getDepth(depth);
        List<DataNodeInfo> dataBundleTree = dataStorageService.listDataEntries(dataBundle.getPath(), dataBundle.getStorageFormat(), depthValue);
        Multistatus propfindResponse = convertBundleTree(dataBundleId, dataBundleTree);
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    private Multistatus convertBundleTree(Number dataBundleId, List<DataNodeInfo> nodeInfoList) {
        Multistatus ms = new Multistatus();
        ms.getResponse().addAll(nodeInfoList.stream()
                .map(nodeInfo -> {
                    Prop prop = new Prop();
                    prop.setContentType(nodeInfo.getMimeType());
                    prop.setContentLength(String.valueOf(nodeInfo.getSize()));
                    prop.setCreationDate(nodeInfo.getCreationTime());
                    prop.setLastmodified(nodeInfo.getLastModified());
                    if (nodeInfo.isCollectionFlag()) {
                        prop.setResourceType("collection");
                    }

                    Propstat propstat = new Propstat();
                    propstat.setProp(prop);
                    propstat.setStatus("HTTP/1.1 200 OK");

                    PropfindResponse propfindResponse = new PropfindResponse();
                    String nodeInfoRelPath = nodeInfo.isCollectionFlag()
                            ?  StringUtils.appendIfMissing(nodeInfo.getNodePath(), "/")
                            : nodeInfo.getNodePath();
                    propfindResponse.setHref(resourceURI.getBaseUriBuilder()
                            .path(AgentStorageResource.AGENTSTORAGE_URI_PATH)
                            .path(dataBundleId.toString())
                            .path(nodeInfoRelPath)
                            .build()
                            .toString());
                    propfindResponse.setPropstat(propstat);
                    return propfindResponse;
                })
                .collect(Collectors.toList()));
        return ms;
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
        long dirEntrySize = dataStorageService.createDirectoryEntry(dataBundle.getPath(), dataDirPath, dataBundle.getStorageFormat());
        long newBundleSize = dataBundle.size() + dirEntrySize;
        storageAllocatorService.updateStorage(
                SecurityUtils.getUserPrincipal(securityContext),
                new JacsBundleBuilder()
                        .dataBundleId(dataBundleId)
                        .usedSpaceInBytes(newBundleSize)
                        .build());
        return Response
                .created(resourceURI.getBaseUriBuilder().path(AgentStorageResource.AGENTSTORAGE_URI_PATH).path("{dataBundleId}").build(dataBundleId))
                .build();
    }

    @LogStorageEvent(
            eventName = "STORAGE_MKCOL",
            argList = {0, 1}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @MKCOL
    @Path("namedDataStorage/{storageName}{format:(/format/[^/]+?)?}")
    public Response createDataStorage(@PathParam("storageName") String storageName,
                                      @PathParam("format") String format,
                                      @Context SecurityContext securityContext) {
        JacsStorageFormat storageFormat;
        if (StringUtils.isBlank(format)) {
            storageFormat = JacsStorageFormat.DATA_DIRECTORY;
        } else {
            storageFormat = JacsStorageFormat.valueOf(format.substring(8)); // 8 is "/format/".length()
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .name(storageName)
                .storageFormat(storageFormat)
                .build();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
        return dataBundleInfo
                .map(bi -> Response
                        .created(resourceURI.getBaseUriBuilder().path(AgentStorageResource.AGENTSTORAGE_URI_PATH).path("{dataBundleId}").build(bi.getId()))
                        .build())
                .orElse(Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ImmutableMap.of("errormessage", "Error allocating the storage"))
                        .build());
    }

    private int getDepth(String depth) {
        if (StringUtils.isBlank(depth) || "infinity".equalsIgnoreCase(depth)) {
            return MAX_ALLOWED_DEPTH;
        } else {
            try {
                int depthValue = Integer.valueOf(depth);
                if (depthValue > 1) {
                    throw new IllegalArgumentException("Illegal depth value - allowed values: {0, 1, infinity}");
                }
                return depthValue;
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }
}
