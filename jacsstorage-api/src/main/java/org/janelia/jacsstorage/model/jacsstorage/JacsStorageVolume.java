package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.util.Date;

@PersistenceInfo(storeName ="jacsStorageVolume", label="JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {

    private String location; // name that uniquely identifies the location
    private String mountHostIP; // IP of the host where the storage volume resides
    private String mountHostURL;
    private String mountPoint;
    private Long capacityInMB;
    private Long availableInMB;
    private Date created = new Date();

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public String getMountHostIP() {
        return mountHostIP;
    }

    public void setMountHostIP(String mountHostIP) {
        this.mountHostIP = mountHostIP;
    }

    public String getMountHostURL() {
        return mountHostURL;
    }

    public void setMountHostURL(String mountHostURL) {
        this.mountHostURL = mountHostURL;
    }

    public String getMountPoint() {
        return mountPoint;
    }

    public void setMountPoint(String mountPoint) {
        this.mountPoint = mountPoint;
    }

    public Long getCapacityInMB() {
        return capacityInMB;
    }

    public void setCapacityInMB(Long capacityInMB) {
        this.capacityInMB = capacityInMB;
    }

    public Long getAvailableInMB() {
        return availableInMB;
    }

    public void setAvailableInMB(Long availableInMB) {
        this.availableInMB = availableInMB;
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
                .append("location", location)
                .append("mountPoint", mountPoint)
                .toString();
    }
}
