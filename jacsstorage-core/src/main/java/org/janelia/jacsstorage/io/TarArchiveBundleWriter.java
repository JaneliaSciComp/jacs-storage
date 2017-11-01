package org.janelia.jacsstorage.io;

import com.google.common.base.Preconditions;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
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
        if (Files.exists(targetPath)) {
            throw new IllegalArgumentException("Target path " + target + " already exists");
        }
        Files.createDirectories(targetPath.getParent());
        return Files.copy(stream, Paths.get(target));
    }

    @Override
    public void createDirectoryEntry(String dataPath, String entryName) {
        Preconditions.checkArgument(StringUtils.isNotBlank(entryName));
        Path rootPath = getRootPath(dataPath);
        if (Files.isDirectory(rootPath)) {
            throw new IllegalArgumentException("Invalid root data path" +
                    rootPath + " is expected to be a tar file not a directory");
        }
        try (RandomAccessFile existingTarFile = new RandomAccessFile(rootPath.toFile(), "rw")) {
            String normalizedEntryName = normalizeEntryName(entryName);
            Path relativeEntryPath = Paths.get(normalizedEntryName);
            Path relativeEntryParentPath = relativeEntryPath.getParent();
            long newEntryPos = newEntryPosition(relativeEntryParentPath != null ? relativeEntryParentPath.toString() : "", existingTarFile);
            if (newEntryPos >= 0) {
                existingTarFile.seek(newEntryPos);
                TarArchiveOutputStream tarArchiveOutputStream = new TarArchiveOutputStream(Channels.newOutputStream(existingTarFile.getChannel()), TarConstants.DEFAULT_RCDSIZE);
                String newEntryName = StringUtils.appendIfMissing(
                        StringUtils.prependIfMissing(
                                StringUtils.prependIfMissing(normalizedEntryName, "/"), "."
                        ),
                        "/" // append '/' since this is a directory entry
                );
                TarArchiveEntry entry = new TarArchiveEntry(newEntryName, false);
                tarArchiveOutputStream.putArchiveEntry(entry);
                tarArchiveOutputStream.closeArchiveEntry();
                tarArchiveOutputStream.finish();
                existingTarFile.close();
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
