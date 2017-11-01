package org.janelia.jacsstorage.webdav;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.service.DataStorageService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.Prop;
import org.janelia.jacsstorage.webdav.propfind.Propfind;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.stream.Collectors;

@Path("agent-webdav")
public class WebdavResource {

    private static int MAX_ALLOWED_DEPTH = 20;

    @Inject
    @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject
    private DataStorageService dataStorageService;

    @Context
    private UriInfo resourceURI;

    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("{dataBundleId}")
    public Response propfind(@PathParam("dataBundleId") Long dataBundleId,
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
                            .path("agent-resource")
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

    @MKCOL
    @Path("{dataBundleId}/{dataDirPath: .+}")
    public Response makeDir(@PathParam("dataBundleId") Long dataBundleId,
                            @PathParam("dataDirPath") String dataDirPath,
                            @Context SecurityContext securityContext) {
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        dataStorageService.createDirectoryEntry(dataBundle.getPath(), dataDirPath, dataBundle.getStorageFormat());
        return Response
                .status(Response.Status.CREATED)
                .build();
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
