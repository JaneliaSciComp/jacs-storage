package org.janelia.jacsstorage.service;

public class NoContentFoundException extends ContentException {
    public NoContentFoundException() {
    }

    public NoContentFoundException(String message) {
        super(message);
    }

    public NoContentFoundException(Throwable cause) {
        super(cause);
    }

    public NoContentFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
