package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.coreutils.PathUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

public class TarArchiveBundleWriter extends AbstractBundleWriter {

    private static final int PARENT_ENTRY_NOT_DIR_ERRORCODE = -1;
    private static final int PARENT_ENTRY_NOT_FOUND_ERRORCODE = -2;
    private static final int DIR_ENTRY_ALREADY_EXISTS_ERRORCODE = -3;
    private static final int FILE_ENTRY_ALREADY_EXISTS_ERRORCODE = -4;
    private static final int UNKNOWN_ERRORCODE = -4;

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @TimedMethod(
            argList = {1},
            logResult = true
    )
    @Override
    protected long writeBundleBytes(InputStream stream, String target) throws Exception {
        Path targetPath = Paths.get(target);
        if (Files.exists(targetPath)) {
            throw new IllegalArgumentException("Target path " + target + " already exists");
        }
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, Paths.get(target));
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long createDirectoryEntry(String dataPath, String entryName) {
        return createNewEntry(dataPath, entryName, 0L,
                (String en) -> {
                    String newEntryName = StringUtils.appendIfMissing(
                            StringUtils.prependIfMissing(
                                    StringUtils.prependIfMissing(en, "/"), "."
                            ),
                            "/" // append '/' since this is a directory entry
                    );
                    return new TarArchiveEntry(newEntryName, false);
                },
                (OutputStream os) -> 0L
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
                            return Files.copy(entryFile, os);
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
            long newEntryPos = newEntryPosition(normalizedEntryName, relativeEntryParentPath != null ? relativeEntryParentPath.toString() : "", existingTarFile);
            if (newEntryPos >= 0) {
                existingTarFile.seek(newEntryPos);
                TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(Channels.newOutputStream(existingTarFile.getChannel()), TarConstants.DEFAULT_RCDSIZE);
                TarArchiveEntry entry = newEntryGenerator.apply(normalizedEntryName);
                entry.setSize(entrySize);
                tarArchiveOutputStream.putArchiveEntry(entry);
                contentProcessor.apply(tarArchiveOutputStream);
                tarArchiveOutputStream.closeArchiveEntry();
                tarArchiveOutputStream.finish();
                long newTarSize = existingTarFile.length();
                return newTarSize - oldTarSize;
            } else if (newEntryPos == PARENT_ENTRY_NOT_DIR_ERRORCODE) {
                throw new IllegalArgumentException("Parent entry found for " + entryName + " but it is not a directory");
            } else if (newEntryPos == PARENT_ENTRY_NOT_FOUND_ERRORCODE){
                throw new IllegalArgumentException("No parent entry found for " + entryName);
            } else if (newEntryPos == DIR_ENTRY_ALREADY_EXISTS_ERRORCODE || newEntryPos == FILE_ENTRY_ALREADY_EXISTS_ERRORCODE) {
                throw new IllegalArgumentException("Entry " + entryName + " already exists");
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

    private long newEntryPosition(String entryName, String parentEntryName, RandomAccessFile tarAccessFile) throws IOException {
        tarAccessFile.seek(0);
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new FileInputStream((tarAccessFile.getFD())));
        int recordSize = tarArchiveInputStream.getRecordSize();
        long prevEntryStart;
        long prevEntryEnd = 0;
        boolean parentEntryNameFound = StringUtils.isBlank(parentEntryName);
        boolean entryNameFound = false;
        boolean parentEntryIsDir = parentEntryNameFound;
        boolean entryFoundIsDir = false;
        for (TarArchiveEntry sourceEntry = tarArchiveInputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = tarArchiveInputStream.getNextTarEntry()) {
            String currentEntryName = normalizeEntryName(sourceEntry.getName());
            if (currentEntryName.equals(parentEntryName)) {
                if (sourceEntry.isDirectory()) {
                    parentEntryNameFound = true;
                    parentEntryIsDir = true;
                } else {
                    parentEntryNameFound = true;
                }
            } else if (currentEntryName.equals(entryName)) {
                entryNameFound = true;
                entryFoundIsDir = sourceEntry.isDirectory();
            }
            prevEntryStart = prevEntryEnd;
            long fillingBytes = sourceEntry.getSize() % recordSize;
            if (fillingBytes > 0) {
                fillingBytes = recordSize - fillingBytes;
            }
            prevEntryEnd = prevEntryStart + recordSize + sourceEntry.getSize() + fillingBytes;
        }
        if (parentEntryNameFound && parentEntryIsDir && !entryNameFound)
            return prevEntryEnd;
        else if (!parentEntryNameFound)
            return PARENT_ENTRY_NOT_FOUND_ERRORCODE;
        else if (parentEntryNameFound && !parentEntryIsDir)
            return PARENT_ENTRY_NOT_DIR_ERRORCODE;
        else if (entryNameFound) {
            if (entryFoundIsDir) {
                return DIR_ENTRY_ALREADY_EXISTS_ERRORCODE;
            } else {
                return FILE_ENTRY_ALREADY_EXISTS_ERRORCODE;
            }
        } else
            return UNKNOWN_ERRORCODE;
    }

}
