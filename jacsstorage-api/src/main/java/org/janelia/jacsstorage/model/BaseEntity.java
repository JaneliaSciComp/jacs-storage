package org.janelia.jacsstorage.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
public interface BaseEntity {
    Number getId();
    void setId(Number id);
    boolean hasId();
    @JsonIgnore
    default String getEntityName() {
        return getClass().getSimpleName();
    };
}
