package org.janelia.jacsstorage.coreutils;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class IOStreamUtils {

    /**
     * Copies from a source path to a stream. The only reason for this is a larger buffer.
     *
     * @param inputStream
     * @param dstStream
     * @return
     * @throws IllegalStateException
     */
    public static long copyFrom(InputStream inputStream, OutputStream dstStream) {
        ReadableByteChannel in = Channels.newChannel(inputStream);
        try {
            return BufferUtils.copy(in, Channels.newChannel(dstStream));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
