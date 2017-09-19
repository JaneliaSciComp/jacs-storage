package org.janelia.jacsstorage.model.jacsstorage;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class JacsDataLocation {

    private final String path;
    private final JacsStorageFormat storageFormat;

    public JacsDataLocation(String path, JacsStorageFormat storageFormat) {
        this.path = path;
        this.storageFormat = storageFormat;
    }

    public String getPath() {
        return path;
    }

    public JacsStorageFormat getStorageFormat() {
        return storageFormat;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("path", path)
                .append("storageFormat", storageFormat)
                .toString();
    }
}
