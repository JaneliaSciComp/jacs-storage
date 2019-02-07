package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class DataDirectoryBundleWriter implements BundleWriter {

    private static final Logger LOG = LoggerFactory.getLogger(DataDirectoryBundleWriter.class);

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.DATA_DIRECTORY);
    }

    @TimedMethod(
            argList = {1},
            logResult = true
    )
    @Override
    public long writeBundle(InputStream stream, String target) {
        long nBytes = 0;
        try {
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new IllegalArgumentException(e);
        }
        return nBytes;
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long createDirectoryEntry(String dataPath, String entryName) {
        return createNewEntry(dataPath, entryName,
                (Path currentEntryPath) -> {
                    throw new DataAlreadyExistException("Entry " + currentEntryPath + " already exists");
                },
                (Path currentEntryPath) -> {
                    try {
                        Files.createDirectory(currentEntryPath);
                        return Files.size(currentEntryPath);
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not create " + currentEntryPath, e);
                    }
                });
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long createFileEntry(String dataPath, String entryName, InputStream contentStream) {
        return createNewEntry(dataPath, entryName,
                (Path currentEntryPath) -> {
                    throw new DataAlreadyExistException("Entry " + currentEntryPath + " already exists");
                },
                (Path currentEntryPath) -> {
                    try {
                        return Files.copy(contentStream, currentEntryPath);
                    } catch (IOException e) {
                        throw new IllegalStateException("Could not create " + currentEntryPath, e);
                    }
                });
    }

    private long createNewEntry(String dataPath, String entryName, Consumer<Path> entryFoundHandler, Function<Path, Long> entryCreator) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootDataPath = getRootPath(dataPath);
        Path entryPath = rootDataPath.resolve(entryName);
        Path parentEntry = entryPath.getParent();
        if (Files.notExists(parentEntry)) {
            createDirIfMissing(parentEntry);
        }
        if (!Files.isDirectory(parentEntry)) {
            throw new IllegalArgumentException("Parent entry found for " + entryPath + " but it is not a directory");
        }
        if (Files.exists(entryPath)) {
            entryFoundHandler.accept(entryPath);
            return 0L;
        } else {
            return entryCreator.apply(entryPath);
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long deleteEntry(String dataPath, String entryName) {
        Path fullEntryPath = Paths.get(dataPath, entryName);
        if (Files.notExists(fullEntryPath)) {
            LOG.info("No file path found for {}, {} ({}) to be deleted", dataPath, entryName, fullEntryPath);
            return 0L;
        }
        long entrySize = PathUtils.getSize(fullEntryPath);
        try {
            PathUtils.deletePath(fullEntryPath);
            LOG.info("Deleted {} from {} ({})", entryName, dataPath, fullEntryPath);
            return entrySize;
        } catch (Exception e) {
            LOG.warn("Error deleting file path {}, {} ({})", dataPath, entryName, fullEntryPath, e);
            return 0;
        }
    }

    private Path getRootPath(String rootDir) {
        Path rootPath = Paths.get(rootDir);
        createDirIfMissing(rootPath);
        return rootPath;
    }

    private void createDirIfMissing(Path dir) {
        if (Files.notExists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                throw new SecurityException("Could not create missing directory - " + dir, e);
            }
        }
    }

}
