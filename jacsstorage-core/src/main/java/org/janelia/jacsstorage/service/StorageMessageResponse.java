package org.janelia.jacsstorage.service;

public class StorageMessageResponse {
    public static final int OK = 0;
    public static final int ERROR = 1;

    private final int status;
    private final String message;
    private final long transferredBytes;
    private final long persistedBytes;
    private final byte[] checksum;

    public StorageMessageResponse(int status, String message, long transferredBytes, long persistedBytes, byte[] checksum) {
        this.status = status;
        this.message = message;
        this.transferredBytes = transferredBytes;
        this.persistedBytes = persistedBytes;
        this.checksum = checksum;
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

    public byte[] getChecksum() {
        return checksum == null ? new byte[0] : checksum;
    }
}
