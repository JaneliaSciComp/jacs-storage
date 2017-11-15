package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@PersistenceInfo(storeName ="jacsStorageVolume", label="JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {
    public static final String OVERFLOW_VOLUME = "OVERFLOW_VOLUME";

    private String storageHost; // storage host
    private String name; // volume name
    private String volumePath; // volume real path
    private List<String> storageTags; // storage tags - identify certain features of the physical storage
    private String storageServiceURL;
    private int storageServiceTCPPortNo;
    private Long availableSpaceInBytes;
    private boolean shared;
    private Date created = new Date();

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

    public String getVolumePath() {
        return volumePath;
    }

    public void setVolumePath(String volumePath) {
        this.volumePath = volumePath;
    }

    public List<String> getStorageTags() {
        return storageTags;
    }

    public void setStorageTags(List<String> storageTags) {
        this.storageTags = storageTags;
    }

    public void addVolumeTag(String tag) {
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

    public int getStorageServiceTCPPortNo() {
        return storageServiceTCPPortNo;
    }

    public void setStorageServiceTCPPortNo(int storageServiceTCPPortNo) {
        this.storageServiceTCPPortNo = storageServiceTCPPortNo;
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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("entityRefId", getEntityRefId())
                .append("storageHost", storageHost)
                .append("name", name)
                .append("storageTags", storageTags)
                .append("volumePath", volumePath)
                .append("availableSpaceInBytes", availableSpaceInBytes)
                .append("storageServiceURL", storageServiceURL)
                .toString();
    }
}
