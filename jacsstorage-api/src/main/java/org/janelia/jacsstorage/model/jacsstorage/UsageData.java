package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.function.Function;

public class UsageData {

    public static final UsageData EMPTY = new UsageData(null, (Number)null, (Number)null, (Number)null, null, null);

    @JsonProperty("lab")
    private final String groupId;
    private final Number spaceUsed;
    private final Number totalSpace;
    private final Number totalFiles;
    private final Double warnPercentage;
    private final Double failPercentage;

    public UsageData(String groupId, String spaceUsed, String totalSpace, String totalFiles,
                     Double warnPercentage, Double failPercentage) {
        this(groupId,
                UsageDataHelper.parseValue("spaceUsed", spaceUsed, BigDecimal::new),
                UsageDataHelper.parseValue("totalSpace", totalSpace, BigDecimal::new),
                UsageDataHelper.parseValue("totalFiles", totalFiles, Long::new),
                warnPercentage,
                warnPercentage);
    }

    private UsageData(String groupId, Number spaceUsed, Number totalSpace, Number totalFiles,
                      Double warnPercentage, Double failPercentage) {
        this.groupId = groupId;
        this.spaceUsed = spaceUsed;
        this.totalSpace = totalSpace;
        this.totalFiles = totalFiles;
        this.warnPercentage = warnPercentage;
        this.failPercentage = warnPercentage;
    }

    public String getGroupId() {
        return groupId;
    }

    public Number getSpaceUsed() {
        return spaceUsed;
    }

    public Number getTotalSpace() {
        return totalSpace;
    }

    public Number getTotalFiles() {
        return totalFiles;
    }

    public UsageData add(UsageData other) {
        String newGroupId;
        if (StringUtils.isBlank(groupId) && StringUtils.isBlank(other.groupId)) {
            newGroupId = null;
        } else if (StringUtils.isBlank(groupId)) {
            newGroupId = other.groupId;
        } else if (StringUtils.isBlank(other.groupId)) {
            newGroupId = groupId;
        } else if (StringUtils.equalsIgnoreCase(groupId, other.groupId)) {
            newGroupId = groupId;
        } else {
            newGroupId = groupId + "," + other.groupId;
        }
        Number newSpaceUsed = UsageDataHelper.addValues(spaceUsed, other.spaceUsed, v -> v);
        Number newTotalSpace = UsageDataHelper.addValues(totalSpace, other.totalSpace, v -> v);
        Number newTotalFiles = UsageDataHelper.addValues(totalFiles, other.totalFiles, v -> v.longValue());
        return new UsageData(newGroupId, newSpaceUsed, newTotalSpace, newTotalFiles,
                UsageDataHelper.minValue(warnPercentage, other.warnPercentage),
                UsageDataHelper.minValue(failPercentage, other.failPercentage));
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("groupId", groupId)
                .append("spaceUsed", spaceUsed)
                .append("totalSpace", totalSpace)
                .append("totalFiles", totalFiles)
                .toString();
    }
}
