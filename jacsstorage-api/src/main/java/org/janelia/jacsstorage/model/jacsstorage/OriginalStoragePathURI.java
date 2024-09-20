package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

@JsonSerialize(using = StoragePathURIJsonSerializer.class)
@JsonDeserialize(using = StoragePathURIJsonDeserializer.class)
public class OriginalStoragePathURI {
    private static final String STORAGE_URI_SCHEME = "jade://";

    /**
     * Creates a StoragePathURI taking the path value exactly as is.
     * @param storagePathValue
     * @return
     */
    public static OriginalStoragePathURI createPathURI(String storagePathValue) {
        return new OriginalStoragePathURI(decodePath(storagePathValue));
    }

    /**
     * Creates a StoragePathURI forcing the path value to an absolute value.
     * @param storagePathValue
     * @return
     */
    public static OriginalStoragePathURI createAbsolutePathURI(String storagePathValue) {
        return StringUtils.isBlank(storagePathValue)
                ? new OriginalStoragePathURI(null)
                : new OriginalStoragePathURI(StringUtils.prependIfMissing(new OriginalStoragePathURI(storagePathValue).getStoragePath(), "/"));
    }

    private static String decodePath(String p) {
        if (StringUtils.isBlank(p)) {
            return p;
        } else {
            try {
                return URLDecoder.decode(p, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Error decoding: " + p + "." + e.getMessage(), e);
            }
        }
    }

    private final String storagePathURI;

    OriginalStoragePathURI(String storagePath) {
        this.storagePathURI = toStoragePathURI(storagePath);
    }

    private String toStoragePathURI(String storagePathValue) {
        if (StringUtils.isBlank(storagePathValue)) {
            return null;
        } else if (storagePathValue.startsWith(STORAGE_URI_SCHEME)) {
            return storagePathValue;
        } else if (storagePathValue.startsWith("jade:/")) {
            return STORAGE_URI_SCHEME + storagePathValue.replaceFirst("^jade:/", "/");
        } else if (storagePathValue.startsWith("//")) {
            return STORAGE_URI_SCHEME + storagePathValue.replaceFirst("^/+", "/");
        } else {
            return STORAGE_URI_SCHEME + storagePathValue;
        }
    }

    public String getStoragePath() {
        if (isEmpty()) {
            return "";
        } else {
            return storagePathURI.substring(STORAGE_URI_SCHEME.length());
        }
    }

    public boolean isEmpty() {
        return storagePathURI == null;
    }

    @Override
    public String toString() {
        return storagePathURI;
    }
}
