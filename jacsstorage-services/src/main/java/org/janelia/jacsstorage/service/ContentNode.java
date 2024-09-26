package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;

public class ContentNode {
    private String prefix;
    private String name;
    private long size;
    private Date lastModified;
    private final ContentReader contentReader;

    public ContentNode(ContentReader contentReader) {
        this.contentReader = contentReader;
    }

    public String getPrefix() {
        return prefix;
    }

    public ContentNode setPrefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public String getName() {
        return name;
    }

    public ContentNode setName(String name) {
        this.name = name;
        return this;
    }

    public long getSize() {
        return size;
    }

    public ContentNode setSize(long size) {
        this.size = size;
        return this;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public ContentNode setLastModified(Date lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    /**
     * @return full object key - this always looks like a full path that starts with '/'
     */
    public String getObjectKey() {
        StringBuilder objectKeyBuilder = new StringBuilder();
        if (StringUtils.isNotBlank(prefix)) {
            objectKeyBuilder.append('/').append(prefix);
        }
        objectKeyBuilder.append('/').append(name);
        return objectKeyBuilder.toString();
    }

    public InputStream getContent() {
        return contentReader.getContentInputstream();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("prefix", prefix)
                .append("name", name)
                .toString();
    }
}
