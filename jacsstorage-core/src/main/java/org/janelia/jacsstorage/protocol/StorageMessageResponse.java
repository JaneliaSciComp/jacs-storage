package org.janelia.jacsstorage.protocol;

public class StorageMessageResponse {
    private final int status;
    private final String message;
    private final long size;

    public StorageMessageResponse(int status, String message, long size) {
        this.status = status;
        this.message = message;
        this.size = size;
    }

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
