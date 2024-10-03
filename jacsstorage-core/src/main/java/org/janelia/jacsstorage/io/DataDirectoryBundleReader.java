package org.janelia.jacsstorage.io;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.collect.ImmutableMap;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.ComparatorUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.rendering.utils.ImageUtils;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataDirectoryBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileBundleReader.class);

    @Inject
    DataDirectoryBundleReader(OriginalContentHandlerProvider contentHandlerProvider) {
        super(contentHandlerProvider);
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.DATA_DIRECTORY);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Map<String, Object> getContentInfo(String source, String entryName) {
        Path sourcePath = getSourcePath(source);
        Preconditions.checkArgument(Files.exists(sourcePath), "No path found for " + source);
        Path entryPath = StringUtils.isBlank(entryName) ? sourcePath : sourcePath.resolve(entryName);
        if (Files.notExists(entryPath)) {
            throw new IllegalArgumentException("No entry " + entryName + " found under " + source + " - " + entryPath + " does not exist");
        }
        try {
            if (Files.isDirectory(entryPath)) {
                return ImmutableMap.of("collectionFlag", true);
            } else {
                ContentInfoExtractor contentInfoExtractor = contentHandlerProvider.getContentInfoExtractor(getMimeType(entryPath));
                return contentInfoExtractor.extractContentInfo(Files.newInputStream(entryPath));
            }
        } catch (Exception e) {
            LOG.error("Error reading content info from {}:{}", source, entryName, e);
            throw new IllegalStateException(e);
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Stream<DataNodeInfo> streamBundleContent(String source, String entryName, int depth) {
        Path sourcePath = getSourcePath(source);
        if (Files.notExists(sourcePath)) {
            return Stream.of();
        }
        Path startPath;
        if (StringUtils.isBlank(entryName)) {
            startPath = sourcePath;
        } else {
            startPath = sourcePath.resolve(entryName);
            if (Files.notExists(startPath)) {
                return Stream.of();
            }
        }
        try {
            // start to collect the files from the startPath but return the data relative to sourcePath
            return Files.walk(startPath, depth, FileVisitOption.FOLLOW_LINKS)
                    .map(p -> pathToDataNodeInfo(sourcePath, p, (rootPath, nodePath) -> rootPath.relativize(nodePath).toString().replace(File.separatorChar, '/')))
                    .filter(dni -> dni != null)
                    ;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long estimateDataEntrySize(String source, String entryName, ContentAccessParams filterParams) {
        Path sourceEntryPath = getSourceEntryPath(source, entryName);
        try {
            ContentConverter contentConverter = contentHandlerProvider.getContentConverter(filterParams);
            DataContent dataContent = getDataContent(sourceEntryPath, filterParams);
            return dataContent == null ? 0L : contentConverter.estimateContentSize(dataContent);
        } catch (Exception e) {
            LOG.error("Error copying data from {}:{}", source, entryName, e);
            throw new IllegalStateException(e);
        }
    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long readDataEntry(String source, String entryName, ContentAccessParams filterParams, OutputStream outputStream) {
        Path sourceEntryPath = getSourceEntryPath(source, entryName);
        try {
            ContentConverter contentConverter = contentHandlerProvider.getContentConverter(filterParams);
            DataContent dataContent = getDataContent(sourceEntryPath, filterParams);
            return dataContent == null ? 0L : contentConverter.convertContent(dataContent, outputStream);
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

    private Path getSourceEntryPath(String source, String entryName) {
        Path sourcePath = getSourcePath(source);
        Preconditions.checkArgument(Files.exists(sourcePath), "No path found for " + source);
        Path entryPath = StringUtils.isBlank(entryName) ? sourcePath : sourcePath.resolve(entryName);
        if (Files.notExists(entryPath)) {
            throw new IllegalArgumentException("No entry " + entryName + " found under " + source + " - " + entryPath + " does not exist");
        }
        return entryPath;
    }

    private DataContent getDataContent(Path entryPath, ContentAccessParams filterParams) {
        DataContent dataContent;
        if (Files.isDirectory(entryPath)) {
            int traverseDepth = filterParams.getMaxDepth() >= 0 ? filterParams.getMaxDepth() : Integer.MAX_VALUE;
            dataContent = new FileListDataContent(
                    filterParams,
                    entryPath,
                    ImageUtils.getImagePathHandler(),
                    () -> {
                        try {
                            Stream<Path> files = Files.walk(entryPath, traverseDepth, FileVisitOption.FOLLOW_LINKS)
                                    .filter(p -> Files.isDirectory(p) || filterParams.matchEntry(p.toString()));
                            if (filterParams.isNaturalSort()) {
                                files = files.sorted((p1, p2) -> ComparatorUtils.naturalCompare(p1.toString(), p2.toString(), true));
                            }
                            if (filterParams.getStartEntryIndex() > 0) {
                                files = files.skip(filterParams.getStartEntryIndex());
                            }
                            if (filterParams.getEntriesCount() > 0) {
                                files = files.limit(filterParams.getEntriesCount());
                            }
                            return files;
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } else {
            if (filterParams.matchEntry(entryPath.toString())) {
                dataContent = new SingleFileDataContent(filterParams, entryPath, ImageUtils.getImagePathHandler());
            } else {
                dataContent = null;
            }
        }
        return dataContent;
    }
}
