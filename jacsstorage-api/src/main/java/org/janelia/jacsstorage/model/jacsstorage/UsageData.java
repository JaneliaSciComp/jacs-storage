package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.function.Supplier;

@JsonIgnoreProperties(ignoreUnknown=true)
public class UsageData {

    private static final BigDecimal TERA = new BigDecimal(1024).pow(4);
    public static final UsageData EMPTY = new UsageData(null, (Number)null, (Number)null, (Number)null, null, null, null);

    @JsonProperty("lab")
    private final String groupId;
    private final Number spaceUsedInBytes;
    private final Number totalSpaceInBytes;
    private final Number totalFiles;
    @JsonProperty
    private final Double warnPercentage;
    @JsonProperty
    private final Double failPercentage;
    @JsonProperty
    private final String userProxy;


    @JsonCreator
    public UsageData(@JsonProperty("lab") String groupId,
                     @JsonProperty("spaceUsedInBytes") String spaceUsedInBytes,
                     @JsonProperty("totalSpaceInBytes") String totalSpaceInBytes,
                     @JsonProperty("totalFiles") String totalFiles,
                     @JsonProperty("warnPercentage") Double warnPercentage,
                     @JsonProperty("failPercentage") Double failPercentage,
                     @JsonProperty("userProxy") String userProxy) {
        this(groupId,
                UsageDataHelper.parseValue("spaceUsedInBytes", spaceUsedInBytes, BigDecimal::new),
                UsageDataHelper.parseValue("totalSpaceInBytes", totalSpaceInBytes, BigDecimal::new),
                UsageDataHelper.parseValue("totalFiles", totalFiles, Long::parseLong),
                warnPercentage,
                failPercentage,
                userProxy);
    }

    private UsageData(String groupId, Number spaceUsedInBytes, Number totalSpaceInBytes, Number totalFiles,
                      Double warnPercentage, Double failPercentage, String userProxy) {
        this.groupId = groupId;
        this.spaceUsedInBytes = spaceUsedInBytes;
        this.totalSpaceInBytes = totalSpaceInBytes;
        this.totalFiles = totalFiles;
        this.warnPercentage = warnPercentage;
        this.failPercentage = failPercentage;
        this.userProxy = userProxy;
    }

    public String getGroupId() {
        return groupId;
    }

    public Number getSpaceUsedInBytes() {
        return spaceUsedInBytes;
    }

    public Number getSpaceUsedTB() {
        return spaceUsedInBytes != null ? new BigDecimal(spaceUsedInBytes.toString()).divide(TERA, 2, RoundingMode.HALF_UP).doubleValue() : 0;
    }

    public Number getTotalSpaceInBytes() {
        return totalSpaceInBytes;
    }

    public Number getTotalSpaceTB() {
        return totalSpaceInBytes != null ? new BigDecimal(totalSpaceInBytes.toString()).divide(TERA, 2, RoundingMode.HALF_UP).doubleValue() : 0;
    }

    public Number getTotalFiles() {
        return totalFiles;
    }

    public String getUserProxy() {
        return userProxy;
    }

    @JsonProperty
    public Double getPercentUsage() {
        return UsageDataHelper.percentage(spaceUsedInBytes, totalSpaceInBytes);
    }

    @JsonProperty
    public String getState() {
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
        Double usageRatio = UsageDataHelper.percentage(spaceUsedInBytes, totalSpaceInBytes);
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
        Number newSpaceUsed = UsageDataHelper.addValues(spaceUsedInBytes, other.spaceUsedInBytes, v -> v);
        Number newTotalSpace = UsageDataHelper.addValues(totalSpaceInBytes, other.totalSpaceInBytes, v -> v);
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
                .append("spaceUsedInBytes", spaceUsedInBytes)
                .append("totalSpaceInBytes", totalSpaceInBytes)
                .append("totalFiles", totalFiles)
                .append("userProxy", userProxy)
                .toString();
    }
}
