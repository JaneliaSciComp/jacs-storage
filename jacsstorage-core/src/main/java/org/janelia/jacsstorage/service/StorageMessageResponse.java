package org.janelia.jacsstorage.service;

public class StorageMessageResponse {
    public static final int OK = 0;
    public static final int ERROR = 1;

    private final int status;
    private final String message;

    public StorageMessageResponse(int status, String message) {
        this.status = status;
        this.message = message;
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
}
