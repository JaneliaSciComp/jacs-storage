package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.support.MongoMapping;

@MongoMapping(collectionName="jacsVolume", label="JacsVolume")
public class JacsVolume extends AbstractEntity {

    private String name; // volume name
    private String mountPoint;
    private Long capacityInMB;
    private Long availableInMB;
    private Number storageId;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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

    public Number getStorageId() {
        return storageId;
    }

    public void setStorageId(Number storageId) {
        this.storageId = storageId;
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
