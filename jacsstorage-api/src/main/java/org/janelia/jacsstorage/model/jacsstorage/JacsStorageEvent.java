package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.model.AbstractEntity;
import org.janelia.jacsstorage.model.annotations.PersistenceInfo;

import java.util.Date;

@PersistenceInfo(storeName ="jacsStorageEvent", label="JacsStorageEvent")
public class JacsStorageEvent extends AbstractEntity {

    private String eventName;
    private String eventDescription;
    private String eventHost;
    private Object eventData;
    private Date eventTime = new Date();

    public String getEventName() {
        return eventName;
    }

    public void setEventName(String eventName) {
        this.eventName = eventName;
    }

    public String getEventDescription() {
        return eventDescription;
    }

    public void setEventDescription(String eventDescription) {
        this.eventDescription = eventDescription;
    }

    public String getEventHost() {
        return eventHost;
    }

    public void setEventHost(String eventHost) {
        this.eventHost = eventHost;
    }

    public Object getEventData() {
        return eventData;
    }

    public void setEventData(Object eventData) {
        this.eventData = eventData;
    }

    public Date getEventTime() {
        return eventTime;
    }

    public void setEventTime(Date eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        JacsStorageEvent that = (JacsStorageEvent) o;

        return new EqualsBuilder()
                .append(eventName, that.eventName)
                .append(eventDescription, that.eventDescription)
                .append(eventData, that.eventData)
                .append(eventTime, that.eventTime)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(eventName)
                .append(eventDescription)
                .append(eventData)
                .append(eventTime)
                .toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("eventName", eventName)
                .append("eventDescription", eventDescription)
                .append("eventData", eventData)
                .append("eventTime", eventTime)
                .toString();
    }
}
