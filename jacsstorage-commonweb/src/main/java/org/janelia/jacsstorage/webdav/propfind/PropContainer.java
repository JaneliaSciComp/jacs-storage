package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.webdav.propfind.customprops.StorageVolumeProp;

import java.util.Date;

public class PropContainer {

    @JacksonXmlProperty(namespace = "xmlns:jade", isAttribute = true)
    private final String namespace = "JADE:";

    @JacksonXmlProperty(localName = "D:resourcetype")
    String resourceType;

    @JacksonXmlProperty(localName = "D:creationdate")
    Date creationDate;

    @JacksonXmlProperty(localName = "D:getlastmodified")
    Date lastmodified;

    @JacksonXmlProperty(localName = "D:displayname")
    String displayname;

    @JacksonXmlProperty(localName = "D:getetag")
    String etag;

    @JacksonXmlProperty(localName = "D:getcontenttype")
    String contentType;

    @JacksonXmlProperty(localName = "D:getcontentlength")
    String contentLength;

    @JacksonXmlProperty(localName = "jade:storageVolume")
    StorageVolumeProp storageVolumeProp;

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

    public String getDisplayname() {
        return displayname;
    }

    public void setDisplayname(String displayname) {
        this.displayname = displayname;
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

    public StorageVolumeProp getStorageVolumeProp() {
        return storageVolumeProp;
    }

    public void setStorageVolumeProp(StorageVolumeProp storageVolumeProp) {
        this.storageVolumeProp = storageVolumeProp;
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
