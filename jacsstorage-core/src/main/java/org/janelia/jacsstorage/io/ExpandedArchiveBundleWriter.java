package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

public class ExpandedArchiveBundleWriter extends AbstractBundleWriter {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.DATA_DIRECTORY);
    }

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

    @Override
    public void createDirectoryEntry(String dataPath, String entryName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootDataPath = getRootPath(dataPath);
        Path entryPath = rootDataPath.resolve(entryName);
        Path parentEntry = rootDataPath.getParent();
        if (Files.notExists(parentEntry)) {
            throw new IllegalArgumentException("A new entry can only be created as a child of an existing entry which must be a directory - " +
                    parentEntry + " does not exist");
        }
        if (!Files.isDirectory(parentEntry)) {
            throw new IllegalArgumentException("A new entry can only be created as a child of an existing entry which must be a directory - " +
                    parentEntry + " is not a directory");
        }
        try {
            Files.createDirectories(entryPath);
        } catch (IOException e) {
            throw new IllegalStateException("Could not create " + entryPath, e);
        }
    }

    private Path getRootPath(String rootDir) {
        Path rootPath = Paths.get(rootDir);
        if (Files.notExists(rootPath)) {
            throw new IllegalArgumentException("No path found for " + rootDir);
        }
        return rootPath;
    }
}
