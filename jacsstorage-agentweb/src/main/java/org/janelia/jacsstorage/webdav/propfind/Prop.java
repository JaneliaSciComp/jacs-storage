package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Date;

public class Prop {
    @JacksonXmlProperty(namespace = "D", localName = "resourcetype")
    String resourceType;

    @JacksonXmlProperty(namespace = "D", localName = "creationdate")
    Date creationDate;

    @JacksonXmlProperty(namespace = "D", localName = "getlastmodified")
    Date lastmodified;

    @JacksonXmlProperty(namespace = "D", localName = "getetag")
    String etag;

    @JacksonXmlProperty(namespace = "D", localName = "getcontenttype")
    String contentType;

    @JacksonXmlProperty(namespace = "D", localName = "getcontentlength")
    String contentLength;

    public String getResourceType() {
        return resourceType;
    }

    public void setResourceType(String resourceType) {
        this.resourceType = resourceType;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Date creationDate) {
        this.creationDate = creationDate;
    }

    public Date getLastmodified() {
        return lastmodified;
    }

    public void setLastmodified(Date lastmodified) {
        this.lastmodified = lastmodified;
    }

    public String getEtag() {
        return etag;
    }

    public void setEtag(String etag) {
        this.etag = etag;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getContentLength() {
        return contentLength;
    }

    public void setContentLength(String contentLength) {
        this.contentLength = contentLength;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("resourceType", resourceType)
                .append("etag", etag)
                .append("contentType", contentType)
                .append("contentLength", contentLength)
                .build();
    }
}
