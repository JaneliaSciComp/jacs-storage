package org.janelia.jacsstorage.rest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;

import javax.ws.rs.core.MultivaluedMap;

class ContentFilterRequestHelper {

    static ContentFilterParams createContentFilterParamsFromQuery(MultivaluedMap<String, String> queryParameters) {
        ContentFilterParams filterParams = new ContentFilterParams();
        queryParameters.forEach((k, vs) -> {
            if ("filterType".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setFilterType(vs.get(0));
            } else if ("selectedEntries".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.addSelectedEntries(vs);
            } else if ("maxDepth".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs)) {
                    filterParams.setMaxDepth(vs.stream().filter(s -> StringUtils.isNotBlank(s)).map(s -> Integer.valueOf(s)).findFirst().orElse(1));
                }
            } else if ("entryPattern".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setEntryNamePattern(vs.get(0));
            } else if (StringUtils.isNotBlank(k) && CollectionUtils.isNotEmpty(vs)) {
                filterParams.addFilterTypeSpecificParam(k, vs.get(0));
            }
        });
        return filterParams;
    }

}
