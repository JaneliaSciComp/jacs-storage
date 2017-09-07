package org.janelia.jacsstorage.service;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStream;

public class TarArchiveBundleReader extends AbstractBundleReader {

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        File sourcePath = new File(source);
        if (!sourcePath.exists()) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!sourcePath.isFile()) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        } else {
            try (ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(new FileInputStream(sourcePath))) {
                return ByteStreams.copy(inputStream, stream);
            }
        }
    }

}
