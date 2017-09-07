package org.janelia.jacsstorage.service;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;

public class SingleFileBundleReader extends AbstractBundleReader {

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        TarArchiveOutputStream outputStream = new TarArchiveOutputStream(stream);
        File sourcePath = new File(source);
        if (!sourcePath.exists()) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!sourcePath.isFile()) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        }
        TarArchiveEntry entry = new TarArchiveEntry(sourcePath);
        outputStream.putArchiveEntry(entry);
        long nBytes = Files.copy(sourcePath.toPath(), outputStream);
        outputStream.closeArchiveEntry();
        outputStream.finish();
        return nBytes;
    }

}
