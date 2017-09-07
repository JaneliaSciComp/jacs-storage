package org.janelia.jacsstorage.service;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class SingleFileBundleWriter extends AbstractBundleWriter {

    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        Path targetPath = Paths.get(target);
        Files.createDirectories(targetPath.getParent());
        ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(stream);
        for (ArchiveEntry sourceEntry = inputStream.getNextEntry(); sourceEntry != null; sourceEntry = inputStream.getNextEntry()) {
            if (!sourceEntry.isDirectory()) {
                return Files.copy(ByteStreams.limit(inputStream, sourceEntry.getSize()), targetPath);
            }
        }
        throw new IllegalArgumentException("No file found in the input archive to be written as " + target);
    }

}
