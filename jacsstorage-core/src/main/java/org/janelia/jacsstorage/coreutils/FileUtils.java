package org.janelia.jacsstorage.coreutils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class FileUtils {

    private static final int BUFFER_SIZE = 32 * 1024;

    /**
     * Copies from a source path to a stream. The only reason for this is a larger buffer.
     * @param srcPath
     * @param dstStream
     * @return
     * @throws IOException
     */
    public static long copyFrom(Path srcPath, OutputStream dstStream)
            throws IOException{
        try (InputStream in = Files.newInputStream(srcPath)) {
            return copy(in, dstStream);
        }
    }

    private static long copy(InputStream source, OutputStream sink)
            throws IOException
    {
        long nread = 0L;
        byte[] buf = new byte[BUFFER_SIZE];
        int n;
        while ((n = source.read(buf)) > 0) {
            sink.write(buf, 0, n);
            nread += n;
        }
        return nread;
    }

}
