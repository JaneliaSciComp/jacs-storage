package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContentFilterParams {
    private String filterType;
    private Set<String> selectedEntries = new HashSet<>();
    private String entryNamePattern;
    private Pattern regexEntryNamePattern;
    private int maxDepth = -1;
    private Map<String, String> filterTypeSpecificParams = new HashMap<>();

    public String getFilterType() {
        return filterType;
    }

    public ContentFilterParams setFilterType(String filterType) {
        this.filterType = filterType;
        return this;
    }

    public Set<String> getSelectedEntries() {
        return selectedEntries;
    }

    public void addSelectedEntries(Collection<String> selectedEntries) {
        this.selectedEntries.addAll(selectedEntries);
    }

    public String getEntryNamePattern() {
        return entryNamePattern;
    }

    public void setEntryNamePattern(String entryNamePattern) {
        this.entryNamePattern = entryNamePattern;
        if (StringUtils.isNotBlank(entryNamePattern)) {
            regexEntryNamePattern = Pattern.compile(entryNamePattern);
        }
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

    boolean matchEntry(String entryName) {
        String nameToMatch = Paths.get(entryName).getFileName().toString();
        return selectedEntries.isEmpty() || selectedEntries.contains(nameToMatch) || matchEntryNamePattern(nameToMatch);
    }

    boolean matchEntryNamePattern(String entryName) {
        if (regexEntryNamePattern == null) {
            return true;
        } else {
            return regexEntryNamePattern.asPredicate().test(entryName);
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("filterType", filterType)
                .append("selectedEntries", selectedEntries)
                .append("maxDepth", maxDepth)
                .append("filterTypeSpecificParams", filterTypeSpecificParams)
                .toString();
    }
}
