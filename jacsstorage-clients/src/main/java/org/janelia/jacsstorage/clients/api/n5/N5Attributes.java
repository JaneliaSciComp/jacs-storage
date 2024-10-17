package org.janelia.jacsstorage.clients.api.n5;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;

public class N5Attributes {
    private final int[] blockSize;
    private final long[] dimensions;
    private final String dataType;
    private final Map<String, String> compression;

    @JsonCreator
    public N5Attributes(int[] blockSize,
                        long[] dimensions,
                        String dataType,
                        Map<String, String> compression) {
        this.blockSize = blockSize;
        this.dimensions = dimensions;
        this.dataType = dataType;
        this.compression = compression;
    }

    public int[] getBlockSize() {
        return blockSize;
    }

    public long[] getDimensions() {
        return dimensions;
    }

    public String getDataType() {
        return dataType;
    }

    public Map<String, String> getCompression() {
        return compression;
    }
}
