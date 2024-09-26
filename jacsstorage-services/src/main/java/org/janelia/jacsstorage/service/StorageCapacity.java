package org.janelia.jacsstorage.service;

public class StorageCapacity {
    private final long totalSpace;
    private final long usableSpace;

    public StorageCapacity(long totalSpace, long usableSpace) {
        this.totalSpace = totalSpace;
        this.usableSpace = usableSpace;
    }

    public long getTotalSpace() {
        return totalSpace;
    }

    public long getUsableSpace() {
        return usableSpace;
    }

}
