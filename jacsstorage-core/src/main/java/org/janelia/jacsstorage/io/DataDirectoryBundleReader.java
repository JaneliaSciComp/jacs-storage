package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DataDirectoryBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileBundleReader.class);

    @Inject
    DataDirectoryBundleReader(ContentHandlerProvider contentHandlerProvider) {
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
            ContentConverter contentConverter = contentHandlerProvider.getContentConverter(filterParams);
            DataContent dataContent;
            if (Files.isDirectory(entryPath)) {
                int traverseDepth = filterParams.getMaxDepth() >= 0 ? filterParams.getMaxDepth() : Integer.MAX_VALUE;
                List<Path> selectedEntries = Files.walk(entryPath, traverseDepth)
                        .filter(p -> Files.isDirectory(p) || filterParams.matchEntry(p.toString()))
                        .collect(Collectors.toList());
                dataContent = new FileListDataContent(filterParams, entryPath, selectedEntries);
            } else {
                if (filterParams.matchEntry(entryPath.toString())) {
                    dataContent = new SingleFileDataContent(filterParams, entryPath);
                } else {
                    return 0L;
                }
            }
            return contentConverter.convertContent(dataContent, outputStream);
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
