package org.janelia.jacsstorage.coreutils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
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
        try (FileChannel in = FileChannel.open(srcPath)) {
            return copy(in, Channels.newChannel(dstStream));
        }
    }

    private static long copy(FileChannel source, WritableByteChannel sink)
            throws IOException {
        long sourceSize = source.size();
        long position = 0L;
        while (position < sourceSize) {
            position += source.transferTo(position, BUFFER_SIZE, sink);
        }
        return sourceSize;
    }

}
