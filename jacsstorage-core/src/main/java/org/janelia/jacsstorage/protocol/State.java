package org.janelia.jacsstorage.protocol;

public enum State {
    IDLE,
    READ_MESSAGE_HEADER,
    READ_DATA_STARTED,
    WRITE_DATA_STARTED,
    READ_DATA,
    WRITE_DATA,
    READ_DATA_COMPLETE,
    READ_DATA_ERROR,
    WRITE_DATA_COMPLETE,
    WRITE_DATA_ERROR
}
