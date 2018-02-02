package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.util.Date;

@PersistenceInfo(storeName ="jacsStorageEvent", label="JacsStorageEvent")
public class JacsStorageEventBuilder extends AbstractEntity {

    private JacsStorageEvent jacsStorageEvent = new JacsStorageEvent();

    public JacsStorageEvent build() {
        JacsStorageEvent toReturn = jacsStorageEvent;
        jacsStorageEvent = new JacsStorageEvent();
        return toReturn;
    }

    public JacsStorageEventBuilder eventName(String v) {
        jacsStorageEvent.setEventName(v);
        return this;
    }

    public JacsStorageEventBuilder eventDescription(String v) {
        jacsStorageEvent.setEventDescription(v);
        return this;
    }

    public JacsStorageEventBuilder eventHost(String v) {
        jacsStorageEvent.setEventHost(v);
        return this;
    }

    public JacsStorageEventBuilder eventData(Object v) {
        jacsStorageEvent.setEventData(v);
        return this;
    }

    public JacsStorageEventBuilder eventStatus(String v) {
        jacsStorageEvent.setEventStatus(v);
        return this;
    }
}
