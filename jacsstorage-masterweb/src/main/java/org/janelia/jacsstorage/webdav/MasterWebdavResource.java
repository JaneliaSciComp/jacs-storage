package org.janelia.jacsstorage.webdav;

import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.helper.StorageResourceHelper;
import org.janelia.jacsstorage.interceptors.annotations.Timed;
import org.janelia.jacsstorage.model.jacsstorage.JADEOptions;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
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

@Timed
@RequireAuthentication
@Path("webdav")
public class MasterWebdavResource {
    private static final Logger LOG = LoggerFactory.getLogger(MasterWebdavResource.class);

    @Inject
    @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject
    @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    @Context
    private UriInfo resourceURI;

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("storage_prefix")
    public Response dataStoragePropFindByQueryParamStoragePrefix(@QueryParam("contentPath") String contentPathParam,
                                                                 Propfind propfindRequest,
                                                                 @Context ContainerRequestContext requestContext,
                                                                 @Context SecurityContext securityContext) {
        return processPropFindDataStorageWithContentPath(contentPathParam, requestContext, securityContext);
    }

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("storage_prefix/{contentPath:.+}")
    public Response dataStoragePropFindByPathParamStoragePrefix(@PathParam("contentPath") String contentPathParam,
                                                                @Context ContainerRequestContext requestContext,
                                                                Propfind propfindRequest,
                                                                @Context SecurityContext securityContext) {
        return processPropFindDataStorageWithContentPath(contentPathParam, requestContext, securityContext);
    }

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("data_storage_path")
    public Response dataStoragePropFindByQueryParamStoragePath(@QueryParam("contentPath") String contentPathParam,
                                                               Propfind propfindRequest,
                                                               @Context ContainerRequestContext requestContext,
                                                               @Context SecurityContext securityContext) {
        return processPropFindDataStorageWithContentPath(contentPathParam, requestContext, securityContext);
    }

    @PROPFIND
    @Consumes(MediaType.APPLICATION_XML)
    @Produces({
            MediaType.APPLICATION_XML, MediaType.APPLICATION_JSON
    })
    @Path("data_storage_path/{contentPath:.+}")
    public Response dataStoragePropFindByPathParamStoragePath(@PathParam("contentPath") String contentPathParam,
                                                              Propfind propfindRequest,
                                                              @Context ContainerRequestContext requestContext,
                                                              @Context SecurityContext securityContext) {
        return processPropFindDataStorageWithContentPath(contentPathParam, requestContext, securityContext);
    }

    private Response processPropFindDataStorageWithContentPath(String contentPathParam,
                                                               ContainerRequestContext requestContext,
                                                               SecurityContext securityContext) {
        LOG.info("Find storage by prefix {} for {}", contentPathParam, securityContext.getUserPrincipal());
        JADEStorageURI jadeStorageURI = JADEStorageURI.createStoragePathURI(
                contentPathParam,
                JADEOptions.create()
                        .setAccessKey(requestContext.getHeaderString("AccessKey"))
                        .setSecretKey(requestContext.getHeaderString("SecretKey"))
                        .setAWSRegion(requestContext.getHeaderString("AWSRegion"))
        );
        StorageResourceHelper resourceHelper = new StorageResourceHelper(storageVolumeManager);
        List<JacsStorageVolume> managedVolumes = resourceHelper.listStorageVolumesForURI(jadeStorageURI);
        if (CollectionUtils.isEmpty(managedVolumes)) {
            LOG.warn("No storage found for prefix {}", contentPathParam);
            Multistatus statusResponse = new Multistatus();
            Propstat propstat = new Propstat();
            propstat.setStatus("HTTP/1.1 404 Not Found");

            PropfindResponse propfindResponse = new PropfindResponse();
            propfindResponse.setResponseDescription("No managed volume found for prefix " + contentPathParam);
            propfindResponse.setPropstat(propstat);
            statusResponse.getResponse().add(propfindResponse);

            return Response
                    .status(Response.Status.NOT_FOUND)
                    .entity(statusResponse)
                    .build();
        }
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(
                managedVolumes,
                requestContext,
                resourceURI.getBaseUriBuilder()
                        .path(ContentStorageResource.class)
                        .path(ContentStorageResource.class, "redirectForGetContentWithQueryParam")
                        .queryParam("contentPath", jadeStorageURI.getJadeStorage())
                        .toString()
        );
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    /**
     * @param storageName
     * @param pathPrefix      - bundle's root relative path to the storage volume, e.g. if the storage volume is /vol/storage and
     *                        pathPrefix is /my/prefix than the bundle's root will be at /vol/storage/my/prefix
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
    @Path("storage/{storageName}/format/DATA_DIRECTORY")
    public Response createDataStorage(@PathParam("storageName") String storageName,
                                      @HeaderParam("pathPrefix") String pathPrefix,
                                      @HeaderParam("storageTags") String storageTags,
                                      @Context SecurityContext securityContext) {
        LOG.info("Create storage {} prefix {} for {}", storageName, pathPrefix, securityContext.getUserPrincipal());
        JacsBundle dataBundle = new JacsBundleBuilder()
                .name(storageName)
                .storageFormat(JacsStorageFormat.DATA_DIRECTORY)
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
