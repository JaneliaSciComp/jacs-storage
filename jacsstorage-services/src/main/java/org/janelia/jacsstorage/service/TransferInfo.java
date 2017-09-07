package org.janelia.jacsstorage.service;

public class TransferInfo {
    private final long numBytes;
    private final byte[] checksum;

    public TransferInfo(long numBytes, byte[] checksum) {
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
