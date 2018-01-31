package org.janelia.jacsstorage.coreutils;

import java.nio.ByteBuffer;

public class BufferUtils {
    public static int copyBuffers(ByteBuffer src, ByteBuffer dst) {
        int nbytes = 0;
        while (src.hasRemaining() && dst.hasRemaining()) {
            dst.put(src.get());
            nbytes++;
        }
        return nbytes;
    }
}
