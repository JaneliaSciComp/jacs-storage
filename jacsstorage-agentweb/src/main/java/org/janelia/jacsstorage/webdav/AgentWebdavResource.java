package org.janelia.jacsstorage.webdav;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.core.UriInfo;

import com.google.common.base.Preconditions;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageOptions;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsStoragePermission;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.requesthelpers.ContentAccessRequestHelper;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.DataContentService;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
import org.janelia.jacsstorage.service.interceptors.annotations.LogStorageEvent;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;
import org.janelia.jacsstorage.webdav.propfind.Multistatus;
import org.janelia.jacsstorage.webdav.propfind.PropContainer;
import org.janelia.jacsstorage.webdav.propfind.Propfind;
import org.janelia.jacsstorage.webdav.propfind.PropfindResponse;
import org.janelia.jacsstorage.webdav.propfind.Propstat;
import org.janelia.jacsstorage.webdav.utils.WebdavUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timed
@RequireAuthentication
@Path(Constants.AGENTSTORAGE_URI_PATH)
public class AgentWebdavResource {

    private static final Logger LOG = LoggerFactory.getLogger(AgentWebdavResource.class);
    private static final int MAX_NODE_ENTRIES = 100000;

    @Inject
    private DataContentService dataContentService;
    @Inject
    @LocalInstance
    private StorageLookupService storageLookupService;
    @Inject
    @LocalInstance
    private StorageVolumeManager storageVolumeManager;

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
        ContentAccessParams contentAccessParams = new ContentAccessParams()
                .setMaxDepth(depthValue)
                .setEntryNamePattern(entryName)
                .setEntriesCount(MAX_NODE_ENTRIES);
        ContentGetter contentGetter = dataContentService.getDataContent(dataBundle.getStorageURI(), contentAccessParams);
        List<PropfindResponse> contentNodesResponses = propfindResponsesFromContentNodes(
                contentGetter.getObjectsList(),
                dataBundle.getStorageVolume().get()
        );
        Multistatus propfindResponse = new Multistatus();
        propfindResponse.getResponse().addAll(contentNodesResponses);
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
    public Response dataStoragePropFindByPathParam(@PathParam("dataPath") String dataPathParam,
                                                   Propfind propfindRequest,
                                                   @Context ContainerRequestContext requestContext,
                                                   @Context UriInfo requestURI,
                                                   @Context SecurityContext securityContext) {
        return processDataStoragePropFind(dataPathParam, requestContext, requestURI, securityContext);
    }

    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @PROPFIND
    @Path("data_storage_path")
    public Response dataStoragePropFindByQueryParam(@QueryParam("contentPath") String dataPathParam,
                                                    Propfind propfindRequest,
                                                    @Context ContainerRequestContext requestContext,
                                                    @Context UriInfo requestURI,
                                                    @Context SecurityContext securityContext) {
        return processDataStoragePropFind(dataPathParam, requestContext, requestURI, securityContext);
    }

    private Response processDataStoragePropFind(String dataPathParam,
                                                ContainerRequestContext requestContext,
                                                UriInfo requestURI,
                                                SecurityContext securityContext) {
        LOG.debug("PROPFIND data storage by path: {}, for {}", dataPathParam, securityContext.getUserPrincipal());
        String depthParam = requestContext.getHeaderString("Depth");
        String accessKeyParam = requestContext.getHeaderString("AccessKey");
        String secretKeyParam = requestContext.getHeaderString("SecretKey");
        StorageResourceHelper storageResourceHelper = new StorageResourceHelper(storageVolumeManager);
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
        JADEStorageOptions storageOptions = new JADEStorageOptions()
                .setAccessKey(accessKeyParam)
                .setSecretKey(secretKeyParam);
        JADEStorageURI contentURI = JADEStorageURI.createStoragePathURI(dataPathParam, storageOptions);
        List<JacsStorageVolume> volumeCandidates;
        try {
            volumeCandidates = storageResourceHelper.listStorageVolumesForURI(contentURI).stream()
                    .filter(storageVolume -> storageVolume.hasPermission(JacsStoragePermission.READ))
                    .collect(Collectors.toList());
        } catch (IllegalArgumentException e) {
            LOG.error("Error retrieving content from {}", contentURI, e);
            // we don't distinguish between "does not exist" and "exist but it has no permissions"
            // we simply return the same result if something is wrong.
            return storageNotFoundHandler.get().build();
        }
        if (CollectionUtils.isEmpty(volumeCandidates)) {
            return storageNotFoundHandler.get().build();
        }
        ContentAccessParams contentAccessParams = ContentAccessRequestHelper.createContentAccessParamsFromQuery(requestURI.getQueryParameters())
                .setMaxDepth(depth);
        return volumeCandidates.stream()
                .findFirst()
                .flatMap(aStorageVolume -> aStorageVolume.resolveAbsoluteLocationURI(contentURI)
                        .map(resolvedContentURI -> Pair.of(aStorageVolume, resolvedContentURI)))
                .map((volAndContentURIPair -> {
                    JacsStorageVolume storageVolume = volAndContentURIPair.getLeft();
                    JADEStorageURI resolvedContentURI = volAndContentURIPair.getRight();
                    ContentGetter contentGetter = dataContentService.getDataContent(resolvedContentURI, contentAccessParams);
                    List<PropfindResponse> contentNodesResponses = propfindResponsesFromContentNodes(
                            contentGetter.getObjectsList(),
                            storageVolume
                    );
                    Multistatus ms = new Multistatus();
                    if (contentNodesResponses.isEmpty()) {
                        Propstat propstat = new Propstat();
                        propstat.setStatus("HTTP/1.1 404 Not Found");

                        PropfindResponse propfindResponse = new PropfindResponse();
                        propfindResponse.setResponseDescription("No path found for " + dataPathParam);
                        propfindResponse.setPropstat(propstat);
                        ms.getResponse().add(propfindResponse);
                        return Response
                                .status(Response.Status.NOT_FOUND)
                                .entity(ms)
                                ;
                    } else {
                        ms.getResponse().addAll(contentNodesResponses);
                        return Response.status(207)
                                .entity(ms)
                                ;
                    }
                }))
                .orElseGet(storageNotFoundHandler)
                .build();
    }

    private List<PropfindResponse> propfindResponsesFromContentNodes(List<ContentNode> contentNodes,
                                                                     JacsStorageVolume storageVolume) {

        return contentNodes.stream()
                .map(contentNode -> {
                    PropContainer propContainer = new PropContainer();
                    propContainer.setContentType(contentNode.getMimeType());
                    propContainer.setEtag(storageVolume.getContentRelativePath(contentNode.getNodeStorageURI()));
                    propContainer.setContentLength(String.valueOf(contentNode.getSize()));
                    propContainer.setLastmodified(contentNode.getLastModified());
                    // set custom properties
                    propContainer.setStorageEntryName(contentNode.getName());
                    propContainer.setStorageBindName(storageVolume.getStorageVirtualPath());
                    propContainer.setStorageRootDir(storageVolume.getStorageRootLocation());

                    Propstat propstat = new Propstat();
                    propstat.setPropContainer(propContainer);
                    propstat.setStatus("HTTP/1.1 200 OK");

                    PropfindResponse propfindResponse = new PropfindResponse();
                    propfindResponse.setHref(resourceURI.getBaseUriBuilder()
                            .path(Constants.AGENTSTORAGE_URI_PATH)
                            .path("storage_path/data_content")
                            .path(contentNode.getNodeStorageURI().getJadeStorage())
                            .build()
                            .toString()
                    );

                    propfindResponse.setPropstat(propstat);

                    return propfindResponse;

                })
                .collect(Collectors.toList());

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
        // this method does not do anything besides checking that the entry exists
        LOG.debug("MKCOL {} : {} for {}", dataBundleId, dataDirPath, securityContext.getUserPrincipal());
        JacsBundle dataBundle = storageLookupService.getDataBundleById(dataBundleId);
        Preconditions.checkArgument(dataBundle != null, "No data bundle found for " + dataBundleId);
        return Response
                .created(resourceURI.getBaseUriBuilder()
                        .path(Constants.AGENTSTORAGE_URI_PATH)
                        .path("{dataBundleId}")
                        .path("data_content")
                        .path("{entryRelativePath}")
                        .build(dataBundleId, dataDirPath))
                .build();
    }

}
