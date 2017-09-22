package org.janelia.jacsstorage.protocol;

public class StorageMessageResponse {
    public static final int OK = 0;
    public static final int ERROR = 1;

    private final int status;
    private final String message;
    private final long size;

    public StorageMessageResponse(int status, String message, long size) {
        this.status = status;
        this.message = message;
        this.size = size;
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

    public long getSize() {
        return size;
    }
}
