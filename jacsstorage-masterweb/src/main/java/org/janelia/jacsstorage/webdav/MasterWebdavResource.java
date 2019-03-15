package org.janelia.jacsstorage.webdav;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.model.jacsstorage.StoragePathURI;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.rest.ContentStorageResource;
import org.janelia.jacsstorage.securitycontext.RequireAuthentication;
import org.janelia.jacsstorage.securitycontext.SecurityUtils;
import org.janelia.jacsstorage.service.StorageAllocatorService;
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

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("storage_prefix/{storagePrefix:.+}")
    public Response dataStoragePropFindByStoragePrefix(@PathParam("storagePrefix") String storagePrefixParam,
                                                       Propfind propfindRequest,
                                                       @Context SecurityContext securityContext) {
        LOG.info("Find storage by prefix {} for {}", storagePrefixParam, securityContext.getUserPrincipal());
        StoragePathURI storagePrefixURI = StoragePathURI.createAbsolutePathURI(storagePrefixParam);
        StorageQuery storageQuery = new StorageQuery().setStorageVirtualPath(storagePrefixURI.getStoragePath());
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        if (CollectionUtils.isEmpty(managedVolumes)) {
            LOG.warn("No storage found for prefix {}", storagePrefixParam);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for prefix " + storagePrefixParam);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        }
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(
                managedVolumes,
                resourceURI.getBaseUriBuilder()
                        .path(ContentStorageResource.class)
                        .path(ContentStorageResource.class, "redirectForContentCheck")
                        .build(storagePrefixURI.toString())
                        .toString()
        );
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("data_storage_path/{dataStoragePath:.+}")
    public Response dataStoragePropFindByStoragePath(@PathParam("dataStoragePath") String dataStoragePathParam,
                                                     Propfind propfindRequest,
                                                     @Context SecurityContext securityContext) {
        LOG.info("Find storage for path {} for {}", dataStoragePathParam, securityContext.getUserPrincipal());
        StoragePathURI dataStoragePathURI = StoragePathURI.createAbsolutePathURI(dataStoragePathParam);
        StorageQuery storageQuery = new StorageQuery().setDataStoragePath(dataStoragePathURI.getStoragePath());
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        if (CollectionUtils.isEmpty(managedVolumes)) {
            LOG.warn("No storage found for path {} - {}", dataStoragePathParam, dataStoragePathURI);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for " + dataStoragePathParam);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        }
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(
                managedVolumes,
                resourceURI.getBaseUriBuilder()
                        .path(ContentStorageResource.class)
                        .path(ContentStorageResource.class, "redirectForContentCheck")
                        .build(dataStoragePathURI.toString())
                        .toString()
        );
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    /**
     * @param storageName
     * @param format
     * @param pathPrefix - bundle's root relative path to the storage volume, e.g. if the storage volume is /vol/storage and
     *                   pathPrefix is /my/prefix than the bundle's root will be at /vol/storage/my/prefix
     * @param storageTags
     * @param securityContext
     * @return
     */
    @LogStorageEvent(
            eventName = "STORAGE_MKCOL",
            argList = {0, 1, 2, 3, 4}
    )
    @MKCOL
    @Produces(MediaType.APPLICATION_JSON)
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
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(pathPrefix, dataBundle, SecurityUtils.getUserPrincipal(securityContext));
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
                .orElseGet(() -> Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ImmutableMap.of("errormessage", "Error allocating the storage"))
                        .build());
    }

}
