package org.janelia.jacsstorage.datatransfer;

import org.apache.commons.lang3.builder.ToStringBuilder;

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("status", status)
                .append("message", message)
                .build();
    }
}
