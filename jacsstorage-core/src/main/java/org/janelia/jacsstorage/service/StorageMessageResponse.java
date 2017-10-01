package org.janelia.jacsstorage.service;

public class StorageMessageResponse {
    public static final int OK = 0;
    public static final int ERROR = 1;

    private final int status;
    private final String message;
    private final long transferredBytes;
    private final long persistedBytes;

    public StorageMessageResponse(int status, String message, long transferredBytes, long persistedBytes) {
        this.status = status;
        this.message = message;
        this.transferredBytes = transferredBytes;
        this.persistedBytes = persistedBytes;
    }

    /**
     * Message status
     * @return 0 for OK - non zero for errors
     */
    public int getStatus() {
        return status;
    }

    public String getMessage() {
        return message == null ? "" : message;
    }

    public long getPersistedBytes() {
        return persistedBytes;
    }

    public long getTransferredBytes() {
        return transferredBytes;
    }
}
