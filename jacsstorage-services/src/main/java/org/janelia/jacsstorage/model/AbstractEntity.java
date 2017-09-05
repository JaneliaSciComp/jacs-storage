package org.janelia.jacsstorage.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AbstractEntity implements BaseEntity {

    @JsonProperty("_id")
    private Number id;

    @Override
    public Number getId() {
        return id;
    }

    @Override
    public void setId(Number id) {
        this.id = id;
    }

    @Override
    public boolean hasId() {
        return id != null;
    }

    @JsonIgnore
    public String getEntityRefId() {
        return getEntityName() + "#" + (hasId() ? getId() : "");
    }

    @Override
    public String toString() {
        return getEntityRefId();
    }
}
