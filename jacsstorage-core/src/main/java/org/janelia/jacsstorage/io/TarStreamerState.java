package org.janelia.jacsstorage.io;

public enum TarStreamerState {
    WRITE_ENTRYHEADER,
    WRITE_ENTRYCONTENT,
    WRITE_ENTRYFOOTER,
    WRITE_COMPLETED
}
