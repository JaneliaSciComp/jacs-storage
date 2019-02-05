package org.janelia.jacsstorage.io;

import org.apache.commons.compress.archivers.ArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DataDirectoryBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileBundleReader.class);

    private static class ArchiveFileVisitor extends SimpleFileVisitor<Path> {
        private final Path parentDir;
        private final ContentStreamFilter contentStreamFilter;
        private final ContentFilterParams filterParams;
        private final ArchiveOutputStream outputStream;

        private long nBytes = 0L;

        ArchiveFileVisitor(Path parentDir, ContentStreamFilter contentStreamFilter, ContentFilterParams filterParams, ArchiveOutputStream outputStream) {
            this.parentDir = parentDir;
            this.contentStreamFilter = contentStreamFilter;
            this.filterParams = filterParams;
            this.outputStream = outputStream;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            createEntry(file);
            nBytes += IOStreamUtils.copyFrom(
                    contentStreamFilter.apply(new ContentFilteredInputStream(filterParams, Files.newInputStream(file))),
                    outputStream);
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

    @Inject
    DataDirectoryBundleReader(ContentStreamFilterProvider contentStreamFilterProvider) {
        super(contentStreamFilterProvider);
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.DATA_DIRECTORY);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<DataNodeInfo> listBundleContent(String source, String entryName, int depth) {
        Path sourcePath = getSourcePath(source);
        if (Files.notExists(sourcePath)) {
            return Collections.emptyList();
        }
        Path startPath;
        if (StringUtils.isBlank(entryName)) {
            startPath = sourcePath;
        } else {
            startPath = sourcePath.resolve(entryName);
            if (Files.notExists(startPath)) {
                return Collections.emptyList();
            }
        }
        try {
            // start to collect the files from the startPath but return the data relative to sourcePath
            return Files.walk(startPath, depth).map(p -> pathToDataNodeInfo(sourcePath, p, (rootPath, nodePath) -> rootPath.relativize(nodePath).toString())).collect(Collectors.toList());
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long readDataEntry(String source, String entryName, ContentFilterParams filterParams, OutputStream outputStream) {
        Path sourcePath = getSourcePath(source);
        Preconditions.checkArgument(Files.exists(sourcePath), "No path found for " + source);
        Path entryPath = StringUtils.isBlank(entryName) ? sourcePath : sourcePath.resolve(entryName);
        if (Files.notExists(entryPath)) {
            throw new IllegalArgumentException("No entry " + entryName + " found under " + source + " - " + entryPath + " does not exist");
        }
        try {
            ContentStreamFilter contentStreamFilter = contentStreamFilterProvider.getContentStreamFilter(filterParams);
            if (Files.isDirectory(entryPath)) {
                TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
                ArchiveFileVisitor archiver = new ArchiveFileVisitor(entryPath, contentStreamFilter, filterParams, tarOutputStream);
                Files.walkFileTree(entryPath, archiver);
                tarOutputStream.finish();
                return archiver.nBytes;
            } else {
                return IOStreamUtils.copyFrom(
                        contentStreamFilter.apply(new ContentFilteredInputStream(filterParams, Files.newInputStream(entryPath))),
                        outputStream);
            }
        } catch (Exception e) {
            LOG.error("Error copying data from {}:{}", source, entryName, e);
            throw new IllegalStateException(e);
        }
    }

    private Path getSourcePath(String source) {
        Path sourcePath = Paths.get(source);
        if (Files.isSymbolicLink(sourcePath)) {
            try {
                return Files.readSymbolicLink(sourcePath);
            } catch (IOException e) {
                LOG.error("Error getting the actual path for symbolic link {}", source, e);
                throw new IllegalStateException(e);
            }
        } else {
            return sourcePath;
        }
    }

}
