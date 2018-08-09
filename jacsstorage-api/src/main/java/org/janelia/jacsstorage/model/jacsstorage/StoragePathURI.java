package org.janelia.jacsstorage.model.jacsstorage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

@JsonSerialize(using = StoragePathURIJsonSerializer.class)
@JsonDeserialize(using = StoragePathURIJsonDeserializer.class)
public class StoragePathURI {
    private static final String STORAGE_URI_SCHEME = "jade://";

    /**
     * Creates a StoragePathURI taking the path value exactly as is.
     * @param storagePathValue
     * @return
     */
    public static StoragePathURI createPathURI(String storagePathValue) {
        return new StoragePathURI(storagePathValue);
    }

    /**
     * Creates a StoragePathURI forcing the path value to an absolute value.
     * @param storagePathValue
     * @return
     */
    public static StoragePathURI createAbsolutePathURI(String storagePathValue) {
        return new StoragePathURI(StringUtils.prependIfMissing(new StoragePathURI(storagePathValue).getStoragePath(), "/"));
    }

    private final String storagePathURI;

    StoragePathURI(String storagePath) {
        this.storagePathURI = toStoragePathURI(storagePath);
    }

    private String toStoragePathURI(String storagePathValue) {
        if (StringUtils.isBlank(storagePathValue)) {
            return null;
        } else if (storagePathValue.startsWith(STORAGE_URI_SCHEME)) {
            return storagePathValue;
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
