package org.janelia.jacsstorage.service;

import java.util.Date;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;

public class ContentNode {
    private static final MimetypesFileTypeMap MIMETYPES_FILE_TYPE_MAP = new MimetypesFileTypeMap();

    private final JacsStorageType storageType;
    private final JADEStorageURI jadeStorageURI;
    private String prefix;
    private String name;
    private long size;
    private Date lastModified;
    private boolean collection;

    public ContentNode(JacsStorageType storageType, JADEStorageURI rootStorageURI) {
        this.storageType = storageType;
        this.jadeStorageURI = rootStorageURI;
    }

    public JacsStorageType getStorageType() {
        return storageType;
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

    public String getMimeType() {
        String ext = PathUtils.getFilenameExt(name);
        if (StringUtils.equalsIgnoreCase(ext, ".lsm")) {
            return "image/tiff";
        } else {
            return MIMETYPES_FILE_TYPE_MAP.getContentType(name);
        }
    }

    public Date getLastModified() {
        return lastModified;
    }

    public ContentNode setLastModified(Date lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    public boolean isCollection() {
        return collection;
    }

    public boolean isNotCollection() {
        return !collection;
    }

    public ContentNode setCollection(boolean collection) {
        this.collection = collection;
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

    public JADEStorageURI getNodeStorageURI() {
        return jadeStorageURI.resolve(getObjectKey());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("storageType", storageType)
                .append("prefix", prefix)
                .append("name", name)
                .toString();
    }
}
