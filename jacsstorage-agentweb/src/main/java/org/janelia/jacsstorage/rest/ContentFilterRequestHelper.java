package org.janelia.jacsstorage.rest;

import org.janelia.jacsstorage.io.ContentFilterParams;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;

class ContentFilterRequestHelper {
    static ContentFilterParams createContentFilterParamsFromQuery(MultivaluedMap<String, String> queryParameters) {
        ContentFilterParams filterParams = new ContentFilterParams();
        // TODO
        return filterParams;
    }
}
