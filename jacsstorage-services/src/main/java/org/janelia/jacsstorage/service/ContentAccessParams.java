package org.janelia.jacsstorage.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ContentAccessParams {

    private static final int DEFAULT_DEPTH = 1;

    private String filterType;
    private final Set<String> selectedEntries = new HashSet<>();
    private String entryNamePattern;
    private Pattern regexEntryNamePattern;
    private int maxDepth = DEFAULT_DEPTH;
    private boolean alwaysArchive;
    private int startEntryIndex;
    private int entriesCount;
    private boolean directoriesOnly;
    private boolean estimateSizeDisabled;
    private final Map<String, String> filterTypeSpecificParams = new HashMap<>();

    public String getFilterType() {
        return filterType;
    }

    public ContentAccessParams setFilterType(String filterType) {
        this.filterType = filterType;
        return this;
    }

    public Set<String> getSelectedEntries() {
        return selectedEntries;
    }

    public ContentAccessParams addSelectedEntry(String selectedEntry) {
        this.selectedEntries.add(selectedEntry);
        return this;
    }

    public ContentAccessParams addSelectedEntries(Collection<String> selectedEntries) {
        this.selectedEntries.addAll(selectedEntries);
        return this;
    }

    public String getEntryNamePattern() {
        return entryNamePattern;
    }

    public ContentAccessParams setEntryNamePattern(String entryNamePattern) {
        this.entryNamePattern = entryNamePattern;
        if (StringUtils.isNotBlank(entryNamePattern)) {
            regexEntryNamePattern = Pattern.compile(entryNamePattern);
        }
        return this;
    }

    public int getMaxDepth() {
        return maxDepth;
    }

    public ContentAccessParams setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    public boolean checkDepth(int currentDepth) {
        return maxDepth < 0 || currentDepth < maxDepth;
    }

    public ContentAccessParams addFilterTypeSpecificParam(String name, String value) {
        if (StringUtils.isNotBlank(value)) {
            filterTypeSpecificParams.put(name, value);
        }
        return this;
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

    public boolean isDirectoriesOnly() {
        return directoriesOnly;
    }

    public ContentAccessParams setDirectoriesOnly(boolean directoriesOnly) {
        this.directoriesOnly = directoriesOnly;
        return this;
    }

    public boolean isAlwaysArchive() {
        return alwaysArchive;
    }

    public ContentAccessParams setAlwaysArchive(boolean alwaysArchive) {
        this.alwaysArchive = alwaysArchive;
        return this;
    }

    public int getStartEntryIndex() {
        return startEntryIndex;
    }

    public ContentAccessParams setStartEntryIndex(int startEntryIndex) {
        this.startEntryIndex = startEntryIndex;
        return this;
    }

    public int getEntriesCount() {
        return entriesCount;
    }

    public ContentAccessParams setEntriesCount(int entriesCount) {
        this.entriesCount = entriesCount;
        return this;
    }

    public boolean isEstimateSizeDisabled() {
        return estimateSizeDisabled;
    }

    public void setEstimateSizeDisabled(boolean estimateSizeDisabled) {
        this.estimateSizeDisabled = estimateSizeDisabled;
    }

    public boolean matchEntry(String entryName) {
        String nameToMatch = Paths.get(entryName).getFileName().toString();
        if (!selectedEntries.isEmpty()) {
            return selectedEntries.contains(nameToMatch);
        } else if (regexEntryNamePattern != null) {
            return regexEntryNamePattern.asPredicate().test(entryName);
        } else {
            return true;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("filterType", filterType)
                .append("selectedEntries", selectedEntries)
                .append("entryNamePattern", entryNamePattern)
                .append("maxDepth", maxDepth)
                .append("filterTypeSpecificParams", filterTypeSpecificParams)
                .toString();
    }
}
