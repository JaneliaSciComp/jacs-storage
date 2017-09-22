package org.janelia.jacsstorage.io;

import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

public class TarArchiveBundleWriter extends AbstractBundleWriter {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        Path targetPath = Paths.get(target);
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, Paths.get(target));
    }

}
