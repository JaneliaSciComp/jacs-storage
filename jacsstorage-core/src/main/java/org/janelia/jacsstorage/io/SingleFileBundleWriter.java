package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

public class SingleFileBundleWriter extends AbstractBundleWriter {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        Path targetPath = Paths.get(target);
        if (Files.exists(targetPath)) {
            throw new IllegalArgumentException("Target path " + target + " already exists");
        }
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, targetPath);
    }

    @Override
    public long createDirectoryEntry(String dataPath, String entryName) {
        throw new UnsupportedOperationException("Method is not supported");
    }

    @Override
    public long createFileEntry(String dataPath, String entryName, InputStream contentStream) {
        Path filePath = Paths.get(dataPath);
        if (Files.exists(filePath)) {
            throw new IllegalArgumentException("File path " + filePath + " already exists");
        }
        try {
            Files.createDirectories(filePath.getParent());
            return Files.copy(contentStream, filePath);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
