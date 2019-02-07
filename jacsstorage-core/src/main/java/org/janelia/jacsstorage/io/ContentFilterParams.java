package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.HashMap;
import java.util.Map;

public class ContentFilterParams {
    private String filterType;
    private String entryName;
    private int maxDepth;
    private Map<String, String> filterTypeSpecificParams = new HashMap<>();

    public String getFilterType() {
        return filterType;
    }

    public ContentFilterParams setFilterType(String filterType) {
        this.filterType = filterType;
        return this;
    }

    public String getEntryName() {
        return entryName;
    }

    public ContentFilterParams setEntryName(String entryName) {
        this.entryName = entryName;
        return this;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public void addFilterTypeSpecificParam(String name, String value) {
        if (StringUtils.isNotBlank(value)) {
            filterTypeSpecificParams.put(name, value);
        }
    }

    public String getAsString(String filterParam, String defaultValue) {
        return StringUtils.defaultIfBlank(filterTypeSpecificParams.get(filterParam), defaultValue);
    }

    public Integer getAsInt(String filterParam, Integer defaultValue) {
        String filterValue = getAsString(filterParam, "");
        if (StringUtils.isNotBlank(filterValue)) {
            try {
                return Integer.valueOf(filterValue.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected numeric value for " + filterParam + " but '" + filterValue + "' is not");
            }
        } else {
            return defaultValue;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("filterType", filterType)
                .append("entryName", entryName)
                .append("maxDepth", maxDepth)
                .append("filterTypeSpecificParams", filterTypeSpecificParams)
                .toString();
    }
}
