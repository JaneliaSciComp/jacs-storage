package org.janelia.jacsstorage.protocol;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

public class StorageMessageHeader {
    private final StorageProtocol.Operation operation;
    private final JacsStorageFormat format;
    private final String location;

    public StorageMessageHeader(StorageProtocol.Operation operation, JacsStorageFormat format, String location) {
        this.operation = operation;
        this.format = format;
        this.location = location;
    }

    public StorageProtocol.Operation getOperation() {
        return operation;
    }

    public JacsStorageFormat getFormat() {
        return format;
    }

    public String getLocation() {
        return location;
    }
}
