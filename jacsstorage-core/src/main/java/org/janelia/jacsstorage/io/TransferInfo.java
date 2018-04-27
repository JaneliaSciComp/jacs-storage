package org.janelia.jacsstorage.io;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.ToStringBuilder;

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

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("numBytes", numBytes)
                .append("checksum", checksum)
                .toString();
    }
}
