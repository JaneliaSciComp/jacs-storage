package org.janelia.jacsstorage.requesthelpers;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;

public class ContentFilterRequestHelper {

    public static ContentFilterParams createContentFilterParamsFromQuery(MultivaluedMap<String, String> queryParameters) {
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
                    filterParams.setMaxDepth(vs.stream().filter(StringUtils::isNotBlank).map(s -> Integer.valueOf(s)).findFirst().orElse(1));
                }
            } else if ("useNaturalSort".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setNaturalSort(Boolean.parseBoolean(vs.get(0)));
            } else if ("noSize".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setEstimateSizeDisabled(Boolean.parseBoolean(vs.get(0)));
            } else if ("alwaysArchive".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setAlwaysArchive(Boolean.parseBoolean(vs.get(0)));
            } else if ("entryPattern".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs))
                    filterParams.setEntryNamePattern(vs.stream().filter(StringUtils::isNotBlank).findFirst().orElse(null));
            } else if ("startEntryIndex".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs)) {
                    String startEntryIndeParamValue = vs.get(0);
                    if (StringUtils.isNotBlank(startEntryIndeParamValue)) {
                        filterParams.setStartEntryIndex(Integer.parseInt(startEntryIndeParamValue.trim()));
                    }
                }
            } else if ("entriesCount".equalsIgnoreCase(k)) {
                if (CollectionUtils.isNotEmpty(vs)) {
                    String entriesCountParamValue = vs.get(0);
                    if (StringUtils.isNotBlank(entriesCountParamValue)) {
                        filterParams.setEntriesCount(Integer.parseInt(entriesCountParamValue.trim()));
                    }
                }
            } else if (StringUtils.isNotBlank(k) && CollectionUtils.isNotEmpty(vs)) {
                filterParams.addFilterTypeSpecificParam(k, vs.get(0));
            }
        });
        return filterParams;
    }

}
