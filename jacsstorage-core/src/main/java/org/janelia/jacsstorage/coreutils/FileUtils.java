package org.janelia.jacsstorage.coreutils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

public class FileUtils {

    private static final int BUFFER_SIZE = 32 * 1024;

    /**
     * Copies from a source path to a stream. The only reason for this is a larger buffer.
     *
     * @param srcPath
     * @param dstStream
     * @return
     * @throws IOException
     */
    public static long copyFrom(Path srcPath, OutputStream dstStream)
            throws IOException {
        try (ReadableByteChannel in = new FileInputStream(srcPath.toFile()).getChannel()) {
            return copy(in, Channels.newChannel(dstStream));
        }
    }

    private static long copy(ReadableByteChannel source, WritableByteChannel sink)
            throws IOException {
        long nread = 0L;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
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
