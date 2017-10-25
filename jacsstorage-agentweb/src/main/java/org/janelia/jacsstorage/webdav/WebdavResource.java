package org.janelia.jacsstorage.webdav;

import org.janelia.jacsstorage.cdi.qualifier.LocalInstance;
import org.janelia.jacsstorage.service.StorageLookupService;
import org.janelia.jacsstorage.webdav.httpverbs.MKCOL;
import org.janelia.jacsstorage.webdav.httpverbs.PROPFIND;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("agent-webdav")
public class WebdavResource {

    @Inject
    @LocalInstance
    private StorageLookupService storageLookupService;

    @Produces(MediaType.APPLICATION_XML)
    @PROPFIND
    @Path("{dataBundleId}")
    public Response propfind(@PathParam("dataBundleId") Long dataBundleId,
                             @Context HttpHeaders headers,
                             @Context HttpServletRequest request) {
        System.out.println("!!!!!!!!!!!!!!!!!!!PROPFIND ID " + dataBundleId);
        System.out.println("!!!!!!!!!!!!!!!!!!!PROPFIND HEADERS " + headers);
        System.out.println("!!!!!!!!!!!!!!!!!!!PROPFIND REQUEST " + request);
        System.out.println("!!!!!!!!!!!!!!!!!!!PROPFIND " + storageLookupService);
        return Response.status(207)
                .build();
    }

    @MKCOL
    @Path("{dataBundleId}")
    public Response makeDir(@PathParam("dataBundleId") Long dataBundleId,
                            @Context HttpHeaders headers,
                            @Context HttpServletRequest request) {
        System.out.println("!!!!!!!!!!!!!!!!!!!MKCOL ID " + dataBundleId);
        System.out.println("!!!!!!!!!!!!!!!!!!!MKCOL HEADERS " + headers);
        System.out.println("!!!!!!!!!!!!!!!!!!!MKCOL REQUEST " + request);
        System.out.println("!!!!!!!!!!!!!!!!!!!MKCOL " + storageLookupService);
        return Response.status(Response.Status.CREATED)
                .build();
    }

}
