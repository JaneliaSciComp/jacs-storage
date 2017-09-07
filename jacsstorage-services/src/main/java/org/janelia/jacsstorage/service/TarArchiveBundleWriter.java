package org.janelia.jacsstorage.service;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TarArchiveBundleWriter extends AbstractBundleWriter {

    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        /**
         * Since the input stream is already expected to be a tar archive simply copy this to the target file.
         * However as a verification step wrap it in an archiveinputstream which automatically checks if the stream
         * is a valid archive.
         */
        ArchiveInputStream inputStream = new ArchiveStreamFactory().createArchiveInputStream(stream);
        Path targetPath = Paths.get(target);
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, Paths.get(target));
    }

}
