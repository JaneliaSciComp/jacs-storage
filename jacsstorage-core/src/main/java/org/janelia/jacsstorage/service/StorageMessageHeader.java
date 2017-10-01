package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

public class StorageMessageHeader {
    private final DataTransferService.Operation operation;
    private final JacsStorageFormat format;
    private final String location;
    private final String message;

    public StorageMessageHeader(DataTransferService.Operation operation, JacsStorageFormat format, String location, String message) {
        this.operation = operation;
        this.format = format;
        this.location = location;
        this.message = message;
    }

    public DataTransferService.Operation getOperation() {
        return operation;
    }

    public JacsStorageFormat getFormat() {
        return format;
    }

    public String getLocationOrDefault() {
        return location == null ? "" : location;
    }

    public String getMessageOrDefault() {
        return message == null ? "" : message;
    }
}
