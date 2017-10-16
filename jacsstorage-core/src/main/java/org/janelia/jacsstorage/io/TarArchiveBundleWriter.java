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
    public boolean checkState(String target) {
        Path targetPath = Paths.get(target);
        if (Files.exists(targetPath)) {
            throw new IllegalStateException("Target path " + target + " already exists");
        }
        return true;
    }

    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        Path targetPath = Paths.get(target);
        if (Files.exists(targetPath)) {
            throw new IllegalArgumentException("Target path " + target + " already exists");
        }
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, Paths.get(target));
    }

}
