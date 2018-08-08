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
        return new StoragePathURI(new StoragePathURI(storagePathValue).asStringPath().map(sp -> StringUtils.prependIfMissing(sp, "/")).orElse(""));
    }

    private final String storagePath;

    StoragePathURI(String storagePath) {
        this.storagePath = toStoragePathURI(storagePath);
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
        return storagePath;
    }

    public Optional<Path> asPath() {
        return asStringPath()
                .map(sp -> Paths.get(sp));
    }

    private Optional<String> asStringPath() {
        if (isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(storagePath.substring(STORAGE_URI_SCHEME.length()));
        }
    }

    public boolean isEmpty() {
        return storagePath == null;
    }

    public String asURIString() {
        return storagePath;
    }

    @Override
    public String toString() {
        return storagePath;
    }
}
