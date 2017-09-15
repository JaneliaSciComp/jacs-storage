package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.support.MongoMapping;

import java.util.Date;

@MongoMapping(collectionName="jacsStorageVolume", label="JacsStorageVolume")
public class JacsStorageVolume extends AbstractEntity {

    private String name; // volume name
    private String location; // location (hostname or IP)
    private String mountPoint;
    private Long capacityInMB;
    private Long availableInMB;
    private Date created = new Date();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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
                .append("name", name)
                .append("mountPoint", mountPoint)
                .toString();
    }
}
