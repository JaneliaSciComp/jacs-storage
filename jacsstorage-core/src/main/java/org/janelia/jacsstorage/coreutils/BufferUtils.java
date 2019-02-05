package org.janelia.jacsstorage.coreutils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

public class BufferUtils {
    public static final int BUFFER_SIZE = 16 * 1024;

    public static int copyBuffers(ByteBuffer src, ByteBuffer dst) {
        int nbytes = 0;
        while (src.hasRemaining() && dst.hasRemaining()) {
            dst.put(src.get());
            nbytes++;
        }
        return nbytes;
    }

    static long copy(ReadableByteChannel source, WritableByteChannel sink)
            throws IOException {
        long nread = 0L;
        final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);
        int n;
        while ((n = source.read(buffer)) > 0) {
            nread += n;
            // prepare the buffer to be drained
            buffer.flip();
            // write to the channel, may block
            sink.write(buffer);
            // If partial transfer, shift remainder down
            // If buffer is empty, same as doing clear()
            buffer.compact();
        }
        // EOF will leave buffer in fill state
        buffer.flip();
        // make sure the buffer is fully drained.
        while (buffer.hasRemaining()) {
            sink.write(buffer);
        }
        return nread;
    }

}
