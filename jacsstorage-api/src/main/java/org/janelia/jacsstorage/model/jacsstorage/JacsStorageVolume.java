package org.janelia.jacsstorage.model.jacsstorage;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.expr.ExprHelper;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

/**
 * Entity for a JACS storage volume.
 * <p>
 * A volume can be shared or local.
 * A local volume corresponds to a local disk that is visible only on the machine where it's installed whereas
 * a shared volume corresponds to a mountable disk that could potentially be mounted on multiple machines (hosts).
 * <p>
 * For local volumes storageHost should be set and shared should be set to false.
 * For shared volumes storageHost is null, shared is set to true.
 */
@PersistenceInfo(storeName = "jacsStorageVolume", label = "JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {
    public static final String GENERIC_S3 = "GenericS3Volume";

    private String storageAgentId; // storage agentId
    // if a volume is set to a network disk that could be mounted on multiple hosts
    private String name; // volume name
    private JacsStorageType storageType; // storage type
    private String storageVirtualPath; // storage path mapping - this will always be formatted as a UNIX path
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
    private boolean activeFlag;
    private Date created = new Date();
    private Date modified = new Date();

    public String getStorageAgentId() {
        return storageAgentId;
    }

    public void setStorageAgentId(String storageAgentId) {
        this.storageAgentId = storageAgentId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JacsStorageType getStorageType() {
        return storageType;
    }

    public void setStorageType(JacsStorageType storageType) {
        this.storageType = storageType;
    }

    public String getStorageVirtualPath() {
        return storageVirtualPath;
    }

    public void setStorageVirtualPath(String storageVirtualPath) {
        this.storageVirtualPath = storageVirtualPath;
    }

    public String getStorageRootTemplate() {
        return storageRootTemplate;
    }

    public void setStorageRootTemplate(String storageRootTemplate) {
        this.storageRootTemplate = storageRootTemplate;
    }

    public String evalStorageRoot(Map<String, Object> evalContext) {
        return storageRootTemplate != null ? ExprHelper.eval(storageRootTemplate, evalContext) : null;
    }

    @JsonIgnore
    public @Nullable String getStorageRootLocation() {
        if (StringUtils.isBlank(storageRootTemplate)) {
            return null;
        } else
            return StringUtils.removeEnd(ExprHelper.getConstPrefix(storageRootTemplate), "/");
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

    public boolean isActiveFlag() {
        return activeFlag;
    }

    public void setActiveFlag(boolean activeFlag) {
        this.activeFlag = activeFlag;
    }

    @JsonIgnore
    public JADEStorageURI getVolumeStorageRootURI() {
        String rootLocation = getStorageRootLocation();
        if (rootLocation == null) {
            return null;
        } else {
            return JADEStorageURI.createStoragePathURI(rootLocation);
        }
    }

    /**
     * @param contentStorageURI
     * @return content's URI relative to the volume's root URI. If the volume's root URI is not set it returns
     * the content's URI as is for S3 and null for file system.
     */
    public String getContentRelativePath(JADEStorageURI contentStorageURI) {
        JADEStorageURI storageRootURI = getVolumeStorageRootURI();
        if (storageRootURI == null) {
            // this is supported only for S3 URIs
            return contentStorageURI.getStorageType() == JacsStorageType.S3 ? contentStorageURI.getJadeStorage() : null;
        }
        return storageRootURI.relativize(contentStorageURI);
    }

    /**
     * Convert an absolute content URI to volume's storage root URI.
     *
     * @param contentStorageURI
     * @return
     */
    public Optional<JADEStorageURI> resolveAbsoluteLocationURI(JADEStorageURI contentStorageURI) {
        JADEStorageURI storageRootURI = getVolumeStorageRootURI();
        if (storageRootURI == null) {
            // this is supported only for S3 URIs
            return contentStorageURI.getStorageType() == JacsStorageType.S3
                    ? Optional.of(contentStorageURI)
                    : Optional.empty();
        }
        if (contentStorageURI.getStorageType() == JacsStorageType.S3) {
            if (storageRootURI.getStorageHost().equals(contentStorageURI.getStorageHost()) &&
                    storageRootURI.relativize(contentStorageURI) != null) {
                // typically storage root for S3 is not defined - we only have a single generic
                // but, it is still possible to set it
                // in case it is set check if a relative path can be created and if it can return the URI as is
                // because it's already relative to this volume's root URI
                return Optional.of(contentStorageURI);
            }
        } else {
            // for file system we compare the path with volume's root
            String contentPath = contentStorageURI.getContentKey();
            if (ExprHelper.match(storageRootTemplate, contentPath).isMatchFound()) {
                String contentRelativePath = storageRootURI.relativizeKey(contentPath);
                if (contentRelativePath != null) {
                    return Optional.of(storageRootURI.resolve(contentRelativePath));
                }
            }
            if (ExprHelper.match(storageVirtualPath, contentPath).isMatchFound()) {
                String contentRelativeToBinding = Paths.get(storageVirtualPath).relativize(Paths.get(contentPath)).toString();
                return Optional.of(storageRootURI.resolve(contentRelativeToBinding));
            }
        }
        return Optional.empty();
    }

    /**
     * Convert a relative content location to an absolute URI.
     *
     * @param locationRelativeToRoot
     * @return
     */
    public Optional<JADEStorageURI> resolveRelativeLocation(String locationRelativeToRoot) {
        JADEStorageURI storageRootURI = getVolumeStorageRootURI();
        if (storageRootURI == null) {
            JADEStorageURI contentStorageURI = JADEStorageURI.createStoragePathURI(locationRelativeToRoot);
            // this is supported only for S3 URIs
            return contentStorageURI.getStorageType() == JacsStorageType.S3
                    ? Optional.of(contentStorageURI)
                    : Optional.empty();
        }
        StringBuilder volumeContentURIBuilder = new StringBuilder(storageRootURI.getJadeStorage());
        if (!locationRelativeToRoot.startsWith("/")) {
            volumeContentURIBuilder.append('/');
        }
        volumeContentURIBuilder.append(locationRelativeToRoot);
        return Optional.of(
                JADEStorageURI.createStoragePathURI(volumeContentURIBuilder.toString())
        );
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("storageAgentId", storageAgentId)
                .append("name", name)
                .append("storageTags", storageTags)
                .append("storageRootTemplate", storageRootTemplate)
                .append("storageVirtualPath", storageVirtualPath)
                .append("storageServiceURL", storageServiceURL)
                .toString();
    }
}
