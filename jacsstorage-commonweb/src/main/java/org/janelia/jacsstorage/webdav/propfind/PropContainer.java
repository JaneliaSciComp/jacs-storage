package org.janelia.jacsstorage.webdav.propfind;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.util.Date;

public class PropContainer {

    @JacksonXmlProperty(localName = "xmlns:jade", isAttribute = true)
    private final String jadePropsNamespace = "JADE:";

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

    @JacksonXmlProperty(localName = "jade:storageBindName")
    String storageBindName;

    @JacksonXmlProperty(localName = "jade:storageEntryName")
    String storageEntryName;

    @JacksonXmlProperty(localName = "jade:storageRootDir")
    String storageRootDir;

    @JacksonXmlProperty(localName = "jade:accessKey")
    String storageAccessKey;

    @JacksonXmlProperty(localName = "jade:secretKey")
    String storageSecretKey;

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

    public String getStorageBindName() {
        return storageBindName;
    }

    public void setStorageBindName(String storageBindName) {
        this.storageBindName = storageBindName;
    }

    public String getStorageEntryName() {
        return storageEntryName;
    }

    public void setStorageEntryName(String storageEntryName) {
        this.storageEntryName = storageEntryName;
    }

    public String getStorageRootDir() {
        return storageRootDir;
    }

    public void setStorageRootDir(String storageRootDir) {
        this.storageRootDir = storageRootDir;
    }

    public String getStorageAccessKey() {
        return storageAccessKey;
    }

    public void setStorageAccessKey(String storageAccessKey) {
        this.storageAccessKey = storageAccessKey;
    }

    public String getStorageSecretKey() {
        return storageSecretKey;
    }

    public void setStorageSecretKey(String storageSecretKey) {
        this.storageSecretKey = storageSecretKey;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("resourceType", resourceType)
                .append("etag", etag)
                .append("contentType", contentType)
                .append("contentLength", contentLength)
                .append("storageBindName", storageBindName)
                .append("storageEntryName", storageEntryName)
                .append("storageRootDir", storageRootDir)
                .build();
    }
}
