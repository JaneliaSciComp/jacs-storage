package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.function.Supplier;

@JsonIgnoreProperties(ignoreUnknown=true)
public class UsageData {

    public static final UsageData EMPTY = new UsageData(null, (Number)null, (Number)null, (Number)null, null, null, null);

    @JsonProperty("lab")
    private final String groupId;
    private final Number spaceUsedTB;
    private final Number totalSpaceTB;
    private final Number totalFiles;
    @JsonProperty
    private final Double warnPercentage;
    @JsonProperty
    private final Double failPercentage;
    @JsonProperty
    private final String userProxy;


    @JsonCreator
    public UsageData(@JsonProperty("lab") String groupId,
                     @JsonProperty("spaceUsedTB") String spaceUsedTB,
                     @JsonProperty("totalSpaceTB") String totalSpaceTB,
                     @JsonProperty("totalFiles") String totalFiles,
                     @JsonProperty("warnPercentage") Double warnPercentage,
                     @JsonProperty("failPercentage") Double failPercentage,
                     @JsonProperty("userProxy") String userProxy) {
        this(groupId,
                UsageDataHelper.parseValue("spaceUsedTB", spaceUsedTB, BigDecimal::new),
                UsageDataHelper.parseValue("totalSpaceTB", totalSpaceTB, BigDecimal::new),
                UsageDataHelper.parseValue("totalFiles", totalFiles, Long::new),
                warnPercentage,
                failPercentage,
                userProxy);
    }

    private UsageData(String groupId, Number spaceUsedTB, Number totalSpaceTB, Number totalFiles,
                      Double warnPercentage, Double failPercentage, String userProxy) {
        this.groupId = groupId;
        this.spaceUsedTB = spaceUsedTB;
        this.totalSpaceTB = totalSpaceTB;
        this.totalFiles = totalFiles;
        this.warnPercentage = warnPercentage;
        this.failPercentage = failPercentage;
        this.userProxy = userProxy;
    }

    public String getGroupId() {
        return groupId;
    }

    public Number getSpaceUsedTB() {
        return spaceUsedTB;
    }

    public Number getTotalSpaceTB() {
        return totalSpaceTB;
    }

    public Number getTotalFiles() {
        return totalFiles;
    }

    public String getUserProxy() {
        return userProxy;
    }

    @JsonProperty
    public Double getPercentUsage() {
        return UsageDataHelper.percentage(spaceUsedTB, totalSpaceTB);
    }

    @JsonProperty("status")
    public String getStatus() {
        Double usageRatio = getPercentUsage();
        if (usageRatio == null) {
            return "UNKNOWN";
        } else {
            Supplier<String> statusFromWarnComparison = () -> {
                Integer warnComparisonResult = UsageDataHelper.compare(usageRatio, warnPercentage);
                if (warnComparisonResult == null) {
                    return "UNKNOWN";
                } else {
                    return warnComparisonResult >= 0 ? "WARN" : "OK";
                }
            };
            Optional<Integer> failComparison = Optional.ofNullable(UsageDataHelper.compare(usageRatio, failPercentage));
            return failComparison.map( failComparisonResult -> {
                if (failComparisonResult >= 0) {
                    return "FAIL";
                } else {
                    return statusFromWarnComparison.get();
                }
            }).orElseGet(statusFromWarnComparison);
        }
    }

    @JsonProperty("details")
    public String getDetails() {
        Double usageRatio = UsageDataHelper.percentage(spaceUsedTB, totalSpaceTB);
        if (usageRatio == null) {
            return "Not available";
        } else {
            Supplier<String> statusFromWarnComparison = () -> {
                Integer warnComparisonResult = UsageDataHelper.compare(usageRatio, warnPercentage);
                if (warnComparisonResult == null) {
                    return "Not available";
                } else {
                    return warnComparisonResult >= 0
                            ? String.format("Quota usage (%.2f%%) exceeds warning threshold (%.2f%%)",
                                    usageRatio * 100, warnPercentage * 100)
                            : String.format("Quota usage is at %.2f%%", usageRatio * 100);
                }
            };
            Optional<Integer> failComparison = Optional.ofNullable(UsageDataHelper.compare(usageRatio, failPercentage));
            return failComparison.map( failComparisonResult -> {
                if (failComparisonResult >= 0) {
                    return String.format("Quota usage (%.2f%%) exceeds max threshold (%.2f%%)",
                            usageRatio * 100, failPercentage * 100);
                } else {
                    return statusFromWarnComparison.get();
                }
            }).orElseGet(statusFromWarnComparison);
        }
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
        Number newSpaceUsed = UsageDataHelper.addValues(spaceUsedTB, other.spaceUsedTB, v -> v);
        Number newTotalSpace = UsageDataHelper.addValues(totalSpaceTB, other.totalSpaceTB, v -> v);
        Number newTotalFiles = UsageDataHelper.addValues(totalFiles, other.totalFiles, v -> v.longValue());
        return new UsageData(newGroupId, newSpaceUsed, newTotalSpace, newTotalFiles,
                UsageDataHelper.minValue(warnPercentage, other.warnPercentage),
                UsageDataHelper.minValue(failPercentage, other.failPercentage),
                other.userProxy);
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("groupId", groupId)
                .append("spaceUsedTB", spaceUsedTB)
                .append("totalSpaceTB", totalSpaceTB)
                .append("totalFiles", totalFiles)
                .append("userProxy", userProxy)
                .toString();
    }
}
