package org.janelia.jacsstorage.io;

public class DataAlreadyExistException extends RuntimeException {
    public DataAlreadyExistException() {
    }

    public DataAlreadyExistException(String message) {
        super(message);
    }

    public DataAlreadyExistException(String message, Throwable cause) {
        super(message, cause);
    }
}
