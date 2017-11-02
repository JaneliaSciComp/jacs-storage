package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.jacsstorage.utils.PathUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;
import java.util.function.Function;

public class TarArchiveBundleWriter extends AbstractBundleWriter {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
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
                        TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(newEntryName, false);
                        tarArchiveEntry.setSize(TarConstants.MAXSIZE);
                        return tarArchiveEntry;
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
        try (RandomAccessFile existingTarFile = new RandomAccessFile(rootPath.toFile(), "rw")) {
            long oldTarSize = existingTarFile.length();
            String normalizedEntryName = normalizeEntryName(entryName);
            Path relativeEntryPath = Paths.get(normalizedEntryName);
            Path relativeEntryParentPath = relativeEntryPath.getParent();
            long newEntryPos = newEntryPosition(relativeEntryParentPath != null ? relativeEntryParentPath.toString() : "", existingTarFile);
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
            } else if (newEntryPos == -1) {
                throw new IllegalArgumentException("Parent entry found for " + entryName + " but it is not a directory");
            } else {
                throw new IllegalArgumentException("No parent entry found for " + entryName);
            }
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private Path getRootPath(String rootDir) {
        Path rootPath = Paths.get(rootDir);
        if (Files.notExists(rootPath)) {
            throw new IllegalArgumentException("No path found for " + rootDir);
        }
        return rootPath;
    }

    private String normalizeEntryName(String name) {
        return StringUtils.removeEnd(
                StringUtils.removeStart(
                        StringUtils.removeStart(name, "."),
                        "/"),
                "/");
    }

    private long newEntryPosition(String entryName, RandomAccessFile tarAccessFile) throws IOException {
        tarAccessFile.seek(0);
        TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(new FileInputStream((tarAccessFile.getFD())));
        int recordSize = tarArchiveInputStream.getRecordSize();
        long prevEntryStart = 0;
        long prevEntryEnd = 0;
        boolean entryNameFound = StringUtils.isBlank(entryName);
        boolean entryIsDir = entryNameFound;
        for (TarArchiveEntry sourceEntry = tarArchiveInputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = tarArchiveInputStream.getNextTarEntry()) {
            String currentEntryName = normalizeEntryName(sourceEntry.getName());
            if (currentEntryName.equals(entryName)) {
                if (sourceEntry.isDirectory()) {
                    entryNameFound = true;
                    entryIsDir = true;
                } else {
                    entryNameFound = true;
                }
            }
            prevEntryStart = prevEntryEnd;
            long fillingBytes = sourceEntry.getSize() % recordSize;
            if (fillingBytes > 0) {
                fillingBytes = recordSize - fillingBytes;
            }
            prevEntryEnd = prevEntryStart + recordSize + sourceEntry.getSize() + fillingBytes;
        }
        if (entryNameFound && entryIsDir)
            return prevEntryEnd;
        else if (entryNameFound)
            return -1;
        else
            return -2;
    }

}
