package org.janelia.jacsstorage.coreutils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtils {

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
        try (ReadableByteChannel in = Files.newByteChannel(srcPath)) {
            return BufferUtils.copy(in, Channels.newChannel(dstStream));
        }
    }

}
