package org.janelia.jacsstorage.rest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedMap;
import java.util.Collection;

class ContentFilterRequestHelper {
    static ContentFilterParams createContentFilterParamsFromQuery(MultivaluedMap<String, String> queryParameters) {
        ContentFilterParams filterParams = new ContentFilterParams();
        queryParameters.forEach((k, vs) -> {
            if ("filterType".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setFilterType(vs.get(0));
            } else if ("entryName".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setEntryName(vs.get(0));
            } else if (StringUtils.isNotBlank(k) && CollectionUtils.isNotEmpty(vs)) {
                filterParams.addFilterTypeSpecificParam(k, vs.get(0));
            }
        });
        return filterParams;
    }
}
