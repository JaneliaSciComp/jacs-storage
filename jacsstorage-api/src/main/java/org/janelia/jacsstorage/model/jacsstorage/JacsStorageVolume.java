package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.expr.MatchingResult;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@PersistenceInfo(storeName ="jacsStorageVolume", label="JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {
    public static final String OVERFLOW_VOLUME = "OVERFLOW_VOLUME";

    private String storageHost; // storage host
    private String name; // volume name
    private String storageVirtualPath; // storage virtual path
    private String storageRootTemplate; // template for storage real root directory
    private List<String> storageTags; // storage tags - identify certain features of the physical storage
    private String storageServiceURL;
    private Long availableSpaceInBytes;
    private Integer percentageFull;
    private Double quotaWarnPercent;
    private Double quotaFailPercent;
    private String systemUsageFile;
    private boolean shared;
    private Set<JacsStoragePermission> volumePermissions;
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

    public String getStorageVirtualPath() {
        return storageVirtualPath;
    }

    public void setStorageVirtualPath(String storageVirtualPath) {
        this.storageVirtualPath = storageVirtualPath;
    }

    @JsonIgnore
    public StoragePathURI getStorageURI() {
        return StoragePathURI.createPathURI(storageVirtualPath);
    }

    public String getStorageRootTemplate() {
        return storageRootTemplate;
    }

    public void setStorageRootTemplate(String storageRootTemplate) {
        this.storageRootTemplate = storageRootTemplate;
    }

    public String evalStorageRootDir(Map<String, Object> evalContext) {
        return ExprHelper.eval(storageRootTemplate, evalContext);
    }

    public String getBaseStorageRootDir() {
        return ExprHelper.getConstPrefix(storageRootTemplate);
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public void setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
    }

    void addStorageTag(String tag) {
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

    @JsonIgnore
    public boolean isNotShared() {
        return !shared;
    }

    public void setShared(boolean shared) {
        this.shared = shared;
    }

    public Set<JacsStoragePermission> getVolumePermissions() {
        return volumePermissions;
    }

    public void setVolumePermissions(Set<JacsStoragePermission> volumePermissions) {
        this.volumePermissions = volumePermissions;
    }

    public boolean hasPermissions() {
        return volumePermissions != null && !volumePermissions.isEmpty();
    }

    public boolean hasPermission(JacsStoragePermission permission) {
        return permission != null && volumePermissions != null && volumePermissions.contains(permission);
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
    public Optional<Path> getRelativePathToBaseStorageRoot(String dataPath) {
        // !!!!!!!!!!!!!!!!!!!!!!!! CHANGE THE RETURN TYPE
        if (ExprHelper.match(storageRootTemplate, dataPath).isMatchFound()) {
            return relativizeToStorageDir(getBaseStorageRootDir(), dataPath);
        } else if (ExprHelper.match(storageVirtualPath, dataPath).isMatchFound()) {
            return relativizeToStorageDir(storageVirtualPath, dataPath);
        } else {
            return Optional.empty();
        }
    }

    @JsonIgnore
    public Path getFullDataPathFromBaseStorageRoot(String dataPathRelativeToBaseStorageRoot) {
        return Paths.get(getBaseStorageRootDir(), dataPathRelativeToBaseStorageRoot);
    }

    private Optional<Path> relativizeToStorageDir(String storageDirName, String dataDirName) {
        if (dataDirName.startsWith(storageDirName)) {
            Path storagePath = Paths.get(storageDirName);
            return Optional.of(storagePath.relativize(Paths.get(dataDirName)));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("storageHost", storageHost)
                .append("name", name)
                .append("storageTags", storageTags)
                .append("storageRootTemplate", storageRootTemplate)
                .append("storageServiceURL", storageServiceURL)
                .append("systemUsageFile", systemUsageFile)
                .toString();
    }
}
