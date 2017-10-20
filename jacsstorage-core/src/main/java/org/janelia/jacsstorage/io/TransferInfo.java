package org.janelia.jacsstorage.io;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TransferInfo {
    private final long numBytes;
    private final byte[] checksum;

    @JsonCreator
    public TransferInfo(@JsonProperty("numBytes") long numBytes, @JsonProperty("checksum") byte[] checksum) {
        this.numBytes = numBytes;
        this.checksum = checksum;
    }

    public long getNumBytes() {
        return numBytes;
    }

    public byte[] getChecksum() {
        return checksum;
    }
}
