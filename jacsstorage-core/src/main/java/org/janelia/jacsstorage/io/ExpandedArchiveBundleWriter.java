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
import java.util.function.Function;

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
    public long createDirectoryEntry(String dataPath, String entryName) {
        return createNewEntry(dataPath, entryName,
                (Path entryPath) -> {
                    try {
                        Files.createDirectory(entryPath);
                        return Files.size(entryPath);
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not create " + entryPath, e);
                    }
                });
    }

    @Override
    public long createFileEntry(String dataPath, String entryName, InputStream contentStream) {
        return createNewEntry(dataPath, entryName,
                (Path entryPath) -> {
                    try {
                        return Files.copy(contentStream, entryPath);
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not create " + entryPath, e);
                    }
                });
    }

    private long createNewEntry(String dataPath, String entryName, Function<Path, Long> entryCreator) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootDataPath = getRootPath(dataPath);
        Path entryPath = rootDataPath.resolve(entryName);
        Path parentEntry = entryPath.getParent();
        if (Files.notExists(parentEntry)) {
            throw new IllegalArgumentException("No parent entry found for " + entryPath);
        }
        if (!Files.isDirectory(parentEntry)) {
            throw new IllegalArgumentException("Parent entry found for " + entryPath + " but it is not a directory");
        }
        if (Files.exists(entryPath)) {
            throw new IllegalArgumentException("Entry " + entryPath + " already exists");
        }
        return entryCreator.apply(entryPath);
    }

    private Path getRootPath(String rootDir) {
        Path rootPath = Paths.get(rootDir);
        if (Files.notExists(rootPath)) {
            throw new IllegalArgumentException("No path found for " + rootDir);
        }
        return rootPath;
    }
}
