package org.janelia.jacsstorage.service;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.security.JacsCredentials;

public class StorageMessageHeader {
    private final Number dataBundleId;
    private final String authToken;
    private final DataTransferService.Operation operation;
    private final JacsStorageFormat format;
    private final String location;
    private final String message;

    public StorageMessageHeader(Number dataBundleId,
                                String authToken,
                                DataTransferService.Operation operation,
                                JacsStorageFormat format,
                                String location,
                                String message) {
        this.dataBundleId = dataBundleId;
        this.authToken = authToken;
        this.operation = operation;
        this.format = format;
        this.location = location;
        this.message = message;
    }

    public Number getDataBundleId() {
        return dataBundleId;
    }

    public String getAuthToken() {
        return authToken;
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
