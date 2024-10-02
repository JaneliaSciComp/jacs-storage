package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.Date;

import javax.activation.MimetypesFileTypeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContentNode {
    private static final Logger LOG = LoggerFactory.getLogger(ContentNode.class);
    private static final MimetypesFileTypeMap MIMETYPES_FILE_TYPE_MAP = new MimetypesFileTypeMap();

    private JacsStorageType storageType;
    private String prefix;
    private String name;
    private long size;
    private Date lastModified;
    private JADEStorageURI jadeStorageURI;
    private final ContentReader contentReader;

    public ContentNode(JacsStorageType storageType, JADEStorageURI rootStorageURI, ContentReader contentReader) {
        LOG.debug("!!!!!! CREATE content node for {}: {}", storageType, rootStorageURI.getJadeStorage());
        this.storageType = storageType;
        this.jadeStorageURI = rootStorageURI;
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
        LOG.debug("!!!!!!! NODE storage for {}:{} URI: {}", this, getObjectKey(), jadeStorageURI.resolve(getObjectKey()));
        return jadeStorageURI.resolve(getObjectKey());
    }

    public InputStream getContent() {
        return contentReader.getContentInputstream();
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
