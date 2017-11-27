package org.janelia.jacsstorage.webdav;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.cdi.qualifier.RemoteInstance;
import org.janelia.jacsstorage.datarequest.StorageQuery;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundle;
import org.janelia.jacsstorage.model.jacsstorage.JacsBundleBuilder;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageVolume;
import org.janelia.jacsstorage.security.SecurityUtils;
import org.janelia.jacsstorage.service.DataStorageService;
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

@Path("webdav")
public class WebdavResource {

    @Inject @RemoteInstance
    private StorageAllocatorService storageAllocatorService;
    @Inject @RemoteInstance
    private StorageVolumeManager storageVolumeManager;
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
    @Path("storagePrefix/{storagePrefix:.+}")
    public Response dataStoragePropFind(@PathParam("storagePrefix") String storagePrefix,
                                        Propfind propfindRequest,
                                        @Context SecurityContext securityContext) {
        StorageQuery storageQuery = new StorageQuery().setStoragePathPrefix(storagePrefix);
        List<JacsStorageVolume> managedVolumes = storageVolumeManager.getManagedVolumes(storageQuery);
        Multistatus propfindResponse = WebdavUtils.convertStorageVolumes(managedVolumes);
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
    @Path("storageName/{storageName}{format:(/format/[^/]+?)?}")
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
                        .created(
                                bi.getStorageVolume()
                                        .map(sv -> sv.getStorageServiceURL())
                                        .map(serviceURL -> UriBuilder.fromUri(serviceURL).path(bi.getId().toString()).build())
                                        .orElseThrow(() -> new IllegalStateException("No volume set for allocated resource " + bi.getId())))
                        .build())
                .orElse(Response
                        .status(Response.Status.NOT_FOUND)
                        .entity(ImmutableMap.of("errormessage", "Error allocating the storage"))
                        .build());
    }

}
