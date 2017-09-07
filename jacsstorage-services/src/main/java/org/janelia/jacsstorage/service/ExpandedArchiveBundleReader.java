package org.janelia.jacsstorage.service;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

public class ExpandedArchiveBundleReader extends AbstractBundleReader {

    private static class ArchiveFileVisitor extends SimpleFileVisitor<Path> {
        private final Path parentDir;
        private final ArchiveOutputStream outputStream;
        private long nBytes = 0L;

        public ArchiveFileVisitor(Path parentDir, ArchiveOutputStream outputStream) {
            this.parentDir = parentDir;
            this.outputStream = outputStream;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            createEntry(file);
            nBytes += Files.copy(file, outputStream);
            outputStream.closeArchiveEntry();
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            createEntry(dir);
            outputStream.closeArchiveEntry();
            return FileVisitResult.CONTINUE;
        }

        private void createEntry(Path p) throws IOException {
            Path entryPath = parentDir.relativize(p);
            TarArchiveEntry entry = new TarArchiveEntry(p.toFile(), entryPath.toString());
            outputStream.putArchiveEntry(entry);
        }
    }
    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        TarArchiveOutputStream outputStream = new TarArchiveOutputStream(stream);
        Path sourcePath = Paths.get(source);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No path found for " + source);
        } else if (!Files.isDirectory(sourcePath)) {
            throw new IllegalArgumentException("Source " + source + " expected to be a directory");
        }
        ArchiveFileVisitor archiver = new ArchiveFileVisitor(sourcePath, outputStream);
        Files.walkFileTree(sourcePath, archiver);
        outputStream.finish();
        return archiver.nBytes;
    }

    private long addFileToArchive(TarArchiveOutputStream tOut, File f, String base) throws IOException {
        String separator = StringUtils.isBlank(base) ? "" : "/";
        String entryName = base + separator + f.getName();
        TarArchiveEntry entry = new TarArchiveEntry(f, entryName);
        tOut.putArchiveEntry(entry);
        long nBytes = 0;
        if (f.isFile()) {
            nBytes = Files.copy(f.toPath(), tOut);
            tOut.closeArchiveEntry();
        } else {
            tOut.closeArchiveEntry();
            File[] dirContent = f.listFiles();
            if (dirContent != null) {
                for (File child : dirContent) {
                    nBytes += addFileToArchive(tOut, child, entryName);
                }
            }
        }
        return nBytes;
    }
}
