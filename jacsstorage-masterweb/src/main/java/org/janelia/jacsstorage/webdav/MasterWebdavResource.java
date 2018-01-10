package org.janelia.jacsstorage.webdav;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.rest.Constants;
import org.janelia.jacsstorage.security.RequireAuthentication;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.LogStorageEvent;
import org.janelia.jacsstorage.service.StorageAllocatorService;
import org.janelia.jacsstorage.service.StorageVolumeManager;
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
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;
import java.util.List;
import java.util.Optional;

@RequireAuthentication
@Path("webdav")
public class MasterWebdavResource {

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;

    @Context
    private UriInfo resourceURI;

    @LogStorageEvent(
            eventName = "DATASTORAGE_PROPFIND",
            argList = {0, 1, 2}
    )
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("storage_prefix/{storagePrefix:.+}")
    public Response dataStoragePropFindByStoragePrefix(@PathParam("storagePrefix") String storagePrefix,
                                                       Propfind propfindRequest,
                                                       @Context SecurityContext securityContext) {
        StorageQuery storageQuery = new StorageQuery().setStoragePathPrefix(storagePrefix);
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(managedVolumes, (storageVolume) ->{
            String storageServiceURL = StringUtils.appendIfMissing(storageVolume.getStorageServiceURL(), "/");
            return storageServiceURL + Constants.AGENTSTORAGE_URI_PATH;
        });
        return Response.status(207)
                .entity(propfindResponse)
                .build();
    }

    @LogStorageEvent(
            eventName = "DATASTORAGE_PROPFIND",
            argList = {0, 1, 2}
    )
    @Consumes(MediaType.APPLICATION_XML)
    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("storage_path/{storagePath:.+}")
    public Response dataStoragePropFindByStoragePath(@PathParam("storagePath") String storagePath,
                                                     Propfind propfindRequest,
                                                     @Context SecurityContext securityContext) {
        StorageQuery storageQuery = new StorageQuery().setDataStoragePath(storagePath);
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
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
            argList = {0, 1}
    )
    @Produces(MediaType.APPLICATION_JSON)
    @MKCOL
    @Path("storage/{storageName}{format:(/format/[^/]+?)?}")
    public Response createDataStorage(@PathParam("storageName") String storageName,
                                      @PathParam("format") String format,
                                      @HeaderParam("storageTags") String storageTags,
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
                .storageTags(storageTags)
                .build();
        Optional<JacsBundle> dataBundleInfo = storageAllocatorService.allocateStorage(SecurityUtils.getUserPrincipal(securityContext), dataBundle);
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
