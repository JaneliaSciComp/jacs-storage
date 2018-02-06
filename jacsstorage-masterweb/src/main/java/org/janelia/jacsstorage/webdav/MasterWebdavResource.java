package org.janelia.jacsstorage.webdav;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ErrorResponse;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Optional;

@Timed
@RequireAuthentication
@Path("webdav")
public class MasterWebdavResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterWebdavResource.class);

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    @Context
    private UriInfo resourceURI;

    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("storage_prefix/{storagePrefix:.+}")
    public Response dataStoragePropFindByStoragePrefix(@PathParam("storagePrefix") String storagePrefix,
                                                       Propfind propfindRequest,
                                                       @Context SecurityContext securityContext) {
        LOG.info("Find storage by prefix {} for {}", storagePrefix, securityContext.getUserPrincipal());
        StorageQuery storageQuery = new StorageQuery().setStoragePathPrefix(storagePrefix);
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        if (CollectionUtils.isEmpty(managedVolumes)) {
            LOG.warn("No storage found for prefix {}", storagePrefix);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for prefix " + storagePrefix);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        }
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(managedVolumes, (storageVolume) ->{
            String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
            return storageServiceURL + Constants.AGENTSTORAGE_URI_PATH;
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("data_storage_path/{dataStoragePath:.+}")
    public Response dataStoragePropFindByStoragePath(@PathParam("dataStoragePath") String dataStoragePath,
                                                     Propfind propfindRequest,
                                                     @Context SecurityContext securityContext) {
        LOG.info("Find storage for path {} for {}", dataStoragePath, securityContext.getUserPrincipal());
        String fullDataStoragePath = StringUtils.prependIfMissing(dataStoragePath, "/");
        StorageQuery storageQuery = new StorageQuery().setDataStoragePath(fullDataStoragePath);
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        if (CollectionUtils.isEmpty(managedVolumes)) {
            LOG.warn("No storage found for path {}", dataStoragePath);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for " + fullDataStoragePath);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        }
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(managedVolumes, (storageVolume) ->{
            String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
            return storageServiceURL + Constants.AGENTSTORAGE_URI_PATH;
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @LogStorageEvent(
            eventName = "STORAGE_MKCOL",
            argList = {0, 1, 2, 3, 4}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @MKCOL
    @Path("storage/{storageName}{format:(/format/[^/]+?)?}")
    public Response createDataStorage(@PathParam("storageName") String storageName,
                                      @PathParam("format") String format,
                                      @HeaderParam("pathPrefix") String pathPrefix,
                                      @HeaderParam("storageTags") String storageTags,
                                      @Context SecurityContext securityContext) {
        LOG.info("Create storage {} format {} prefix {} for {}", storageName, format, pathPrefix, securityContext.getUserPrincipal());
        JacsStorageFormat storageFormat;
        if (StringUtils.isBlank(format)) {
            storageFormat = JacsStorageFormat.DATA_DIRECTORY;
        } else {
            storageFormat = JacsStorageFormat.valueOf(format.substring(8)); // 8 is "/format/".length()
        }
        JacsBundle dataBundle = new JacsBundleBuilder()
                .name(storageName)
                .storageFormat(storageFormat)
                .storageTags(storageTags)
                .build();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(SecurityUtils.getUserPrincipal(securityContext), pathPrefix, dataBundle);
        return dataBundleInfo
                .map(bi -> Response
                        .created(
                                bi.getStorageVolume()
                                        .map(sv -> sv.getStorageServiceURL())
                                        .map(serviceURL -> UriBuilder
                                                .fromUri(serviceURL)
                                                .path(Constants.AGENTSTORAGE_URI_PATH)
                                                .path(bi.getId().toString())
                                                .build())
                                        .orElseThrow(() -> new IllegalStateException("No volume set for allocated resource " + bi.getId())))
                        .build())
                .orElse(Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ImmutableMap.of("errormessage", "Error allocating the storage"))
                        .build());
    }

}
