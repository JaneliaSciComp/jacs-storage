package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@PersistenceInfo(storeName ="jacsStorageVolume", label="JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {
    public static final String OVERFLOW_VOLUME = "OVERFLOW_VOLUME";

    private String storageHost; // storage host
    private String name; // volume name
    private String storagePathPrefix; // storage path prefix
    private String storageRootDir; // storage real root directory path
    private List<String> storageTags; // storage tags - identify certain features of the physical storage
    private String storageServiceURL;
    private Long availableSpaceInBytes;
    private Integer percentageFull;
    private Double quotaWarnPercent;
    private Double quotaFailPercent;
    private String systemUsageFile;
    private boolean shared;
    private Date created = new Date();
    private Date modified = new Date();

    public String getStorageHost() {
        return storageHost;
    }

    public void setStorageHost(String storageHost) {
        this.storageHost = storageHost;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getStoragePathPrefix() {
        return storagePathPrefix;
    }

    public void setStoragePathPrefix(String storagePathPrefix) {
        this.storagePathPrefix = storagePathPrefix;
    }

    public String getStorageRootDir() {
        return storageRootDir;
    }

    public void setStorageRootDir(String storageRootDir) {
        this.storageRootDir = storageRootDir;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public void setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
    }

    public void addStorageTag(String tag) {
        if (storageTags == null) {
            storageTags = new ArrayList<>();
        }
        if (StringUtils.isNotBlank(tag)) {
            storageTags.add(tag);
        }
    }

    public boolean hasTags() {
        return storageTags != null && !storageTags.isEmpty();
    }

    public String getStorageServiceURL() {
        return storageServiceURL;
    }

    public void setStorageServiceURL(String storageServiceURL) {
        this.storageServiceURL = storageServiceURL;
    }

    public Long getAvailableSpaceInBytes() {
        return availableSpaceInBytes;
    }

    public void setAvailableSpaceInBytes(Long availableSpaceInBytes) {
        this.availableSpaceInBytes = availableSpaceInBytes;
    }

    public boolean hasAvailableSpaceInBytes() {
        return availableSpaceInBytes != null && availableSpaceInBytes > 0;
    }

    public Integer getPercentageFull() {
        return percentageFull;
    }

    public void setPercentageFull(Integer percentageFull) {
        this.percentageFull = percentageFull;
    }

    public boolean hasPercentageAvailable() {
        return percentageFull != null;
    }

    public Double getQuotaWarnPercent() {
        return quotaWarnPercent;
    }

    public void setQuotaWarnPercent(Double quotaWarnPercent) {
        this.quotaWarnPercent = quotaWarnPercent;
    }

    public Double getQuotaFailPercent() {
        return quotaFailPercent;
    }

    public void setQuotaFailPercent(Double quotaFailPercent) {
        this.quotaFailPercent = quotaFailPercent;
    }

    public String getSystemUsageFile() {
        return systemUsageFile;
    }

    public void setSystemUsageFile(String systemUsageFile) {
        this.systemUsageFile = systemUsageFile;
    }

    public boolean isShared() {
        return shared;
    }

    public void setShared(boolean shared) {
        this.shared = shared;
    }

    public Date getCreated() {
        return created;
    }

    public void setCreated(Date created) {
        this.created = created;
    }

    public Date getModified() {
        return modified;
    }

    public void setModified(Date modified) {
        this.modified = modified;
    }

    @JsonIgnore
    public Path getStorageRelativePath(String dataPath) {
        Path relativePath = null;
        if (storageRootDir.length() > storagePathPrefix.length()) {
            relativePath = relativizeToStorageDir(dataPath, storageRootDir);
            if (relativePath != null) {
                return relativePath;
            } else {
                return relativizeToStorageDir(dataPath, storagePathPrefix);
            }
        } else {
            relativePath = relativizeToStorageDir(dataPath, storagePathPrefix);
            if (relativePath != null) {
                return relativePath;
            } else {
                return relativizeToStorageDir(dataPath, storageRootDir);
            }
        }
    }

    private Path relativizeToStorageDir(String dataDirName, String storageDirName) {
        if (dataDirName.startsWith(storageDirName)) {
            Path storagePath = Paths.get(storageDirName);
            return storagePath.relativize(Paths.get(dataDirName));
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("storageHost", storageHost)
                .append("name", name)
                .append("storageTags", storageTags)
                .append("storageRootDir", storageRootDir)
                .append("availableSpaceInBytes", availableSpaceInBytes)
                .append("storageServiceURL", storageServiceURL)
                .append("systemUsageFile", systemUsageFile)
                .toString();
    }
}
