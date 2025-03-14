package org.janelia.jacsstorage.clients.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Optional;

/**
 * This is the equivalent of the jade.DataNodeInfo type.
 */
public class StorageEntryInfo {
    private final String storageId;
    private final String storageURL;
    private final String entryURL;
    private final String storageRootLocation;
    private final StoragePathURI storageRootPathURI;
    private final String entryRelativePath;
    private final Long size;
    private final boolean collectionFlag;
    private final String mimeType;

    @JsonCreator
    public StorageEntryInfo(@JsonProperty("storageId") String storageId,
                            @JsonProperty("storageURL") String storageURL,
                            @JsonProperty("nodeAccessURL") String entryURL,
                            @JsonProperty("storageRootLocation") String storageRootLocation,
                            @JsonProperty("storageRootPathURI") StoragePathURI storageRootPathURI,
                            @JsonProperty("nodeRelativePath") String entryRelativePath,
                            @JsonProperty("size") Long size,
                            @JsonProperty("collectionFlag") boolean collectionFlag,
                            @JsonProperty("mimeType") String mimeType) {
        this.storageId = storageId;
        this.storageURL = storageURL;
        this.entryURL = entryURL;
        this.storageRootLocation = storageRootLocation;
        this.storageRootPathURI = storageRootPathURI;
        this.entryRelativePath = entryRelativePath;
        this.size = size;
        this.collectionFlag = collectionFlag;
        this.mimeType = mimeType;
    }

    public String getStorageId() {
        return storageId;
    }

    public String getStorageURL() {
        return storageURL;
    }

    public String getEntryURL() {
        return entryURL;
    }

    @JsonIgnore
    public String getDecodedEntryURL() {
        if (StringUtils.isEmpty(entryURL)) {
            return entryURL;
        } else {
            try {
                return URLDecoder.decode(entryURL, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException(e);
            }
        }
    }

    public String getStorageRootLocation() {
        return storageRootLocation;
    }

    public StoragePathURI getStorageRootPathURI() {
        return storageRootPathURI;
    }

    public String getEntryRelativePath() {
        return entryRelativePath;
    }

    public Optional<StoragePathURI> getEntryPathURI() {
        return storageRootPathURI.resolve(entryRelativePath);
    }

    public Long getSize() {
        return size;
    }

    boolean isCollectionFlag() {
        return collectionFlag;
    }

    @JsonIgnore
    public boolean isCollection() {
        return isCollectionFlag();
    }

    @JsonIgnore
    public boolean isNotCollection() {
        return !isCollectionFlag();
    }

    public String getMimeType() {
        return mimeType;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("storageId", storageId)
                .append("storageURL", storageURL)
                .append("entryURL", entryURL)
                .append("storageRootLocation", storageRootLocation)
                .append("storageRootPathURI", storageRootPathURI)
                .append("entryRelativePath", entryRelativePath)
                .append("collectionFlag", collectionFlag)
                .toString();
    }
}
