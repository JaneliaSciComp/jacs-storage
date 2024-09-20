package org.janelia.jacsstorage.service;

public class ContentException extends RuntimeException {
    public ContentException() {
    }

    public ContentException(String message) {
        super(message);
    }

    public ContentException(Throwable cause) {
        super(cause);
    }

    public ContentException(String message, Throwable cause) {
        super(message, cause);
    }
}
