package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.support.MongoMapping;

import java.util.Date;

@MongoMapping(collectionName="jacsStorage", label="JacsStorage")
public class JacsStorage extends AbstractEntity {

    private String location; // storage location
    private Date created = new Date();

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
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
                .toString();
    }
}
