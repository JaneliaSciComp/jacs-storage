package org.janelia.jacsstorage.service;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ExpandedArchiveBundleWriter extends AbstractBundleWriter {

    @Override
    public long writeBundleBytes(InputStream stream, String target) throws Exception {
        long nBytes = 0;
        ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(stream);
        Path targetPath = Paths.get(target);
        Files.createDirectories(targetPath);
        for (ArchiveEntry sourceEntry = inputStream.getNextEntry(); sourceEntry != null; sourceEntry = inputStream.getNextEntry()) {
            Path targetEntryPath = targetPath.resolve(sourceEntry.getName());
            if (sourceEntry.isDirectory()) {
                Files.createDirectories(targetEntryPath);
            } else {
                nBytes += Files.copy(ByteStreams.limit(inputStream, sourceEntry.getSize()), targetEntryPath);
            }
        }
        return nBytes;
    }

}
