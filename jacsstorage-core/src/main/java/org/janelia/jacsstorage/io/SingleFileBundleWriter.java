package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.EnumSet;
import java.util.Set;

public class SingleFileBundleWriter implements BundleWriter {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @TimedMethod(
            argList = {1},
            logResult = true
    )
    @Override
    public long writeBundle(InputStream stream, String target) {
        Path targetPath = Paths.get(target);
        if (Files.exists(targetPath)) {
            throw new DataAlreadyExistException("Target path " + target + " already exists");
        }
        try {
            Files.createDirectories(targetPath.getParent());
            return Files.copy(stream, targetPath);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long createDirectoryEntry(String dataPath, String entryName) {
        throw new UnsupportedOperationException("Method is not supported");
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long createFileEntry(String dataPath, String entryName, InputStream contentStream) {
        Path filePath = Paths.get(dataPath);
        if (Files.exists(filePath)) {
            throw new DataAlreadyExistException("File path " + filePath + " already exists");
        }
        try {
            Files.createDirectories(filePath.getParent());
            return Files.copy(contentStream, filePath);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

}
