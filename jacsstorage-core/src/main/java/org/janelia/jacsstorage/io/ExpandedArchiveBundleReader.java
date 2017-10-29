package org.janelia.jacsstorage.io;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            String entryName = StringUtils.prependIfMissing(
                    StringUtils.prependIfMissing(entryPath.toString(), "/"),
                    ".");
            TarArchiveEntry entry = new TarArchiveEntry(p.toFile(), entryName);
            outputStream.putArchiveEntry(entry);
        }
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.DATA_DIRECTORY);
    }

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        TarArchiveOutputStream outputStream = new TarArchiveOutputStream(stream);
        Path sourcePath = getSourcePath(source);
        Path archiverRootDir;
        if (Files.isRegularFile(sourcePath)) {
            archiverRootDir = sourcePath.getParent();
        } else {
            archiverRootDir = sourcePath;
        }
        ArchiveFileVisitor archiver = new ArchiveFileVisitor(archiverRootDir, outputStream);
        Files.walkFileTree(sourcePath, archiver);
        outputStream.finish();
        return archiver.nBytes;
    }

    @Override
    public List<DataNodeInfo> listBundleContent(String source, int depth) {
        Path sourcePath = getSourcePath(source);
        try {
            return Files.walk(sourcePath, depth).map(p -> pathToDataNodeInfo(sourcePath, p)).collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long readDataEntry(String source, String entryName, OutputStream outputStream) throws IOException {
        Path sourcePath = getSourcePath(source);
        if (StringUtils.isBlank(entryName)) {
            try {
                return readBundleBytes(source, outputStream);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        Path entryPath = sourcePath.resolve(entryName);
        if (Files.notExists(entryPath)) {
            throw new IllegalArgumentException("No entry " + entryName + " found under " + source + " - " + entryPath + " does not exist");
        }
        if (Files.isDirectory(entryPath)) {
            TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(outputStream);
            Path archiverRootDir;
            if (Files.isRegularFile(sourcePath)) {
                archiverRootDir = sourcePath.getParent();
            } else {
                archiverRootDir = sourcePath;
            }
            ArchiveFileVisitor archiver = new ArchiveFileVisitor(archiverRootDir, tarOutputStream);
            Files.walkFileTree(sourcePath, archiver);
            tarOutputStream.finish();
            return archiver.nBytes;
        } else {
            return Files.copy(entryPath, outputStream);
        }
    }

    private Path getSourcePath(String source) {
        Path sourcePath = Paths.get(source);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No path found for " + source);
        }
        return sourcePath;
    }

}
