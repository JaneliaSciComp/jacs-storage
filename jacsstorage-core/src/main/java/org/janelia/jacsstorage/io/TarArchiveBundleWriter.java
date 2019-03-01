package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.BufferUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TarArchiveBundleWriter implements BundleWriter {

    private static final Logger LOG = LoggerFactory.getLogger(TarArchiveBundleWriter.class);

    private static class NewEntrySearchPosResult {
        private static final int OK_FOR_NEW_ENTRY_POS = 0;
        private static final int PARENT_ENTRY_NOT_DIR_ERRORCODE = -1;
        private static final int DIR_ENTRY_ALREADY_EXISTS_ERRORCODE = -2;
        private static final int FILE_ENTRY_ALREADY_EXISTS_ERRORCODE = -3;

        private final int searchResult;
        private final List<String> missingDirs;
        private final long newEntryPos;

        private NewEntrySearchPosResult(int searchResult, List<String> missingDirs, long newEntryPos) {
            this.searchResult = searchResult;
            this.missingDirs = missingDirs;
            this.newEntryPos = newEntryPos;
        }
    }

    private static class StreamChunk {
        private final long startPos;
        private final long size;

        private StreamChunk(long startPos, long size) {
            this.startPos = startPos;
            this.size = size;
        }
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
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
            return Files.copy(stream, Paths.get(target));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long createDirectoryEntry(String dataPath, String entryName) {
        return createNewEntry(dataPath, entryName, 0L,
                (String en) -> {
                    String newEntryName = createDirectoryEntryName(en);
                    return new TarArchiveEntry(newEntryName, false);
                },
                (OutputStream os) -> 0L
        );
    }

    private String createDirectoryEntryName(String entryName) {
        return StringUtils.appendIfMissing(
                StringUtils.prependIfMissing(
                        StringUtils.prependIfMissing(entryName, "/"), "."
                ),
                "/" // append '/' since this is a directory entry
        );
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long createFileEntry(String dataPath, String entryName, InputStream contentStream) {
        Path tempPath = null;
        try {
            tempPath = Files.createTempDirectory("tempTarEntry");
            Path entryFile = tempPath.resolve(entryName.replaceAll("/", "_"));
            Files.copy(contentStream, entryFile);
            return createNewEntry(dataPath, entryName, Files.size(entryFile),
                    (en) -> {
                        String newEntryName = StringUtils.prependIfMissing(
                                StringUtils.prependIfMissing(en, "/"),
                                "."
                        );
                        return new TarArchiveEntry(newEntryName, false);
                    },
                    (os) -> {
                        try {
                            return PathUtils.copyFileToStream(entryFile, os);
                        } catch (IOException e) {
                            throw new IllegalStateException(e);
                        }
                    }
            );
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (tempPath != null) {
                try {
                    PathUtils.deletePath(tempPath);
                } catch (IOException ignore) {
                }
            }
        }
    }

    private long createNewEntry(String dataPath, String entryName, long entrySize,
                                Function<String, TarArchiveEntry> newEntryGenerator,
                                Function<OutputStream, Long> contentProcessor) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootPath = getRootPath(dataPath);
        if (Files.isDirectory(rootPath)) {
            throw new IllegalArgumentException("Invalid root data path" +
                    rootPath + " is expected to be a tar file not a directory");
        }
        FileLock tarLock = null;
        try (RandomAccessFile existingTarFile = new RandomAccessFile(rootPath.toFile(), "rw")) {
            tarLock = lockTarFile(existingTarFile.getChannel());
            long oldTarSize = existingTarFile.length();
            String normalizedEntryName = normalizeEntryName(entryName);
            Path relativeEntryPath = Paths.get(normalizedEntryName);
            Path relativeEntryParentPath = relativeEntryPath.getParent();
            NewEntrySearchPosResult newEntryPosResult = newEntryPosition(normalizedEntryName, relativeEntryParentPath, existingTarFile);
            if (newEntryPosResult.searchResult == NewEntrySearchPosResult.OK_FOR_NEW_ENTRY_POS) {
                existingTarFile.seek(newEntryPosResult.newEntryPos);
                TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(Channels.newOutputStream(existingTarFile.getChannel()), TarConstants.DEFAULT_RCDSIZE);
                for (String missingDir : newEntryPosResult.missingDirs) {
                    String newEntryName = createDirectoryEntryName(missingDir);
                    TarArchiveEntry missingDirEntry = new TarArchiveEntry(newEntryName, false);
                    missingDirEntry.setSize(entrySize);
                    tarArchiveOutputStream.putArchiveEntry(missingDirEntry);
                    tarArchiveOutputStream.closeArchiveEntry();
                }
                TarArchiveEntry entry = newEntryGenerator.apply(normalizedEntryName);
                entry.setSize(entrySize);
                tarArchiveOutputStream.putArchiveEntry(entry);
                contentProcessor.apply(tarArchiveOutputStream);
                tarArchiveOutputStream.closeArchiveEntry();
                tarArchiveOutputStream.finish();
                long newTarSize = existingTarFile.length();
                return newTarSize - oldTarSize;
            } else if (newEntryPosResult.searchResult == NewEntrySearchPosResult.PARENT_ENTRY_NOT_DIR_ERRORCODE) {
                throw new IllegalArgumentException("Parent entry found for " + entryName + " but it is not a directory");
            } else if (newEntryPosResult.searchResult == NewEntrySearchPosResult.DIR_ENTRY_ALREADY_EXISTS_ERRORCODE || newEntryPosResult.searchResult == NewEntrySearchPosResult.FILE_ENTRY_ALREADY_EXISTS_ERRORCODE) {
                throw new DataAlreadyExistException("Entry " + entryName + " already exists");
            } else {
                throw new IllegalStateException("Unknown error condition while trying to create new entry " + entryName);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (tarLock != null) {
                try {
                    tarLock.release();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public long deleteEntry(String dataPath, String entryName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootPath = getRootPath(dataPath);
        if (Files.notExists(rootPath)) {
            LOG.info("Delete {}, {} - no file path found for {}", dataPath, entryName, rootPath);
            return 0;
        } else if (Files.isDirectory(rootPath)) {
            LOG.info("Delete {}, {} - Root {} is a directory", dataPath, entryName, rootPath);
            return 0;
        }
        FileLock tarLock = null;
        try (RandomAccessFile existingTarFile = new RandomAccessFile(rootPath.toFile(), "rw")) {
            tarLock = lockTarFile(existingTarFile.getChannel());
            List<StreamChunk> entriesToBeDeleted = getEntriesToDelete(existingTarFile, entryName);
            if (entriesToBeDeleted.isEmpty()) {
                return 0L;
            } else {
                long oldTarSize = existingTarFile.length();
                List<StreamChunk> copiedIntervals = Streams.zip(
                        Stream.concat(Stream.of(new StreamChunk(0L, 0L)), entriesToBeDeleted.stream()),
                        Stream.concat(entriesToBeDeleted.stream(), Stream.of(new StreamChunk(oldTarSize, 0L))),
                        (de1, de2) -> new StreamChunk(de1.startPos + de1.size, de2.startPos - de1.startPos - de1.size))
                        .collect(Collectors.toList());
                FileChannel tarChannel = existingTarFile.getChannel();
                long writePos = 0L;
                final ByteBuffer buffer = ByteBuffer.allocateDirect(BufferUtils.BUFFER_SIZE);
                for (StreamChunk toCopy : copiedIntervals) {
                    StreamChunk currentChunk = new StreamChunk(toCopy.startPos, toCopy.size);
                    while (currentChunk.size > 0) {
                        if (currentChunk.size < buffer.remaining()) {
                            buffer.limit((int) currentChunk.size);
                        }
                        int nbytesRead = tarChannel.read(buffer, currentChunk.startPos);
                        currentChunk = new StreamChunk(currentChunk.startPos + nbytesRead, currentChunk.size - nbytesRead);
                        buffer.flip();
                        while (buffer.hasRemaining()) {
                            int nbytesWritten = tarChannel.write(buffer, writePos);
                            writePos += nbytesWritten;
                        }
                        buffer.compact();
                    }
                }
                tarChannel.truncate(writePos);
                LOG.info("Deleted {} from {}", entryName, dataPath);
                return oldTarSize - writePos;
            }
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            if (tarLock != null) {
                try {
                    tarLock.release();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    private List<StreamChunk> getEntriesToDelete(RandomAccessFile tarFile, String entryName) throws IOException {
        String normalizedEntryName = normalizeEntryName(entryName);
        FileChannel tarChannel = tarFile.getChannel();
        tarChannel.position(0L); // go to the beginning of the file just in case the FP was moved
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(Channels.newInputStream(tarChannel));
        List<StreamChunk> deletedEntriesPos = new ArrayList<>();
        for (TarArchiveEntry sourceEntry = tarArchiveInputStream.getNextTarEntry();
             sourceEntry != null;
             sourceEntry = tarArchiveInputStream.getNextTarEntry()) {
            long currentEntryPos = tarChannel.position();
            String currentEntryName = normalizeEntryName(sourceEntry.getName());
            if (currentEntryName.equals(normalizedEntryName) ||
                    currentEntryName.startsWith(normalizedEntryName + "/")) {
                long entryPos = currentEntryPos - TarConstants.DEFAULT_RCDSIZE;
                long entrySize = sourceEntry.getSize() + TarConstants.DEFAULT_RCDSIZE;
                if (entrySize % TarConstants.DEFAULT_RCDSIZE != 0) {
                    // tar entry size should be an exact multiple of the record size
                    entrySize = ((entrySize + TarConstants.DEFAULT_RCDSIZE) / TarConstants.DEFAULT_RCDSIZE) * TarConstants.DEFAULT_RCDSIZE;
                }
                deletedEntriesPos.add(new StreamChunk(entryPos, entrySize));
            }
        }
        return deletedEntriesPos;
    }

    private Path getRootPath(String rootDir) {
        Path rootPath = Paths.get(rootDir);
        if (Files.notExists(rootPath)) {
            Path parentDir = rootPath.getParent();
            if (parentDir != null) {
                try {
                    Files.createDirectories(rootPath.getParent());
                } catch (IOException e) {
                    throw new SecurityException("Could not create parent directory for missing archive - " + rootDir, e);
                }
            }
        }
        return rootPath;
    }

    private FileLock lockTarFile(FileChannel tarFileChannel) throws IOException{
        FileLock lock = null;
        for (;;) {
            try {
                try {
                    lock = tarFileChannel.tryLock();
                } catch (OverlappingFileLockException e) {
                    // capture exceptions thrown when the lock is held by the current JVM
                    continue;
                }
            } finally {
                if (lock != null) {
                    break;
                }
            }
        }
        return lock;
    }

    private String normalizeEntryName(String name) {
        if (StringUtils.startsWith(name, "/")) {
            return StringUtils.removeEnd(StringUtils.removeStart(name, "/"), "/");
        } else {
            return StringUtils.removeEnd(StringUtils.removeStart(name, "./"), "/");
        }
    }

    private NewEntrySearchPosResult newEntryPosition(String entryName, Path parentEntryPath, RandomAccessFile tarAccessFile) throws IOException {
        tarAccessFile.seek(0);
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new FileInputStream((tarAccessFile.getFD())));
        int recordSize = tarArchiveInputStream.getRecordSize();
        long prevEntryStart;
        long prevEntryEnd = 0;
        List<String> entryParentHierarchy;
        if (parentEntryPath == null) {
            // (all) parent(s) exist automatically in this case
            entryParentHierarchy = new ArrayList<>();
        } else {
            int nPathComponents = parentEntryPath.getNameCount();
            entryParentHierarchy = IntStream.rangeClosed(1, nPathComponents)
                    .mapToObj(i -> parentEntryPath.subpath(0, i))
                    .map(p -> p.toString())
                    .collect(Collectors.toList());
        }
        boolean entryNameFound = false;
        boolean entryFoundIsDir = false;
        boolean parentEntryNotDir = false;
        for (TarArchiveEntry sourceEntry = tarArchiveInputStream.getNextTarEntry();
             sourceEntry != null;
             sourceEntry = tarArchiveInputStream.getNextTarEntry()) {
            String currentEntryName = normalizeEntryName(sourceEntry.getName());
            if (currentEntryName.equals(entryName)) {
                entryNameFound = true;
                entryFoundIsDir = sourceEntry.isDirectory();
            } else {
                if (sourceEntry.isDirectory()) {
                    entryParentHierarchy.remove(currentEntryName);
                } else {
                    if (entryParentHierarchy.contains(currentEntryName)) {
                        // currentEntry is in the parent hierarchy and is not a directory
                        entryParentHierarchy.remove(currentEntryName);
                        parentEntryNotDir = true;
                    }
                }
            }
            prevEntryStart = prevEntryEnd;
            long fillingBytes = sourceEntry.getSize() % recordSize;
            if (fillingBytes > 0) {
                fillingBytes = recordSize - fillingBytes;
            }
            prevEntryEnd = prevEntryStart + recordSize + sourceEntry.getSize() + fillingBytes;
        }
        int errorCode;
        if (parentEntryNotDir) {
            errorCode = NewEntrySearchPosResult.PARENT_ENTRY_NOT_DIR_ERRORCODE;
        } else if (entryNameFound) {
            if (entryFoundIsDir) {
                errorCode = NewEntrySearchPosResult.DIR_ENTRY_ALREADY_EXISTS_ERRORCODE;
            } else {
                errorCode = NewEntrySearchPosResult.FILE_ENTRY_ALREADY_EXISTS_ERRORCODE;
            }
        } else {
            errorCode = NewEntrySearchPosResult.OK_FOR_NEW_ENTRY_POS;
        }
        return new NewEntrySearchPosResult(errorCode, entryParentHierarchy, prevEntryEnd);
    }

}
