package org.janelia.jacsstorage.io;

import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.PathUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.janelia.rendering.utils.ImageUtils;
import org.msgpack.core.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public class SingleFileBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(SingleFileBundleReader.class);

    @Inject
    public SingleFileBundleReader(OriginalContentHandlerProvider contentHandlerProvider) {
        super(contentHandlerProvider);
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Map<String, Object> getContentInfo(String source, String entryName) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        try {
            ContentInfoExtractor contentInfoExtractor = contentHandlerProvider.getContentInfoExtractor(getMimeType(sourcePath));
            return contentInfoExtractor.extractContentInfo(Files.newInputStream(sourcePath));
        } catch (IOException e) {
            LOG.error("Error reading content info from {}", source, e);
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
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        return Stream.of(pathToDataNodeInfo(sourcePath, sourcePath, (rootPath, nodePath) -> rootPath.toString()))
                .filter(dni -> dni != null);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long estimateDataEntrySize(String source, String entryName, ContentFilterParams filterParams) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        return PathUtils.getSize(sourcePath, 0);
    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long readDataEntry(String source, String entryName, ContentFilterParams filterParams, OutputStream outputStream) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        ContentConverter contentConverter = contentHandlerProvider.getContentConverter(filterParams);
        DataContent dataContent = new SingleFileDataContent(filterParams, sourcePath, ImageUtils.getImagePathHandler());
        return contentConverter.convertContent(dataContent, outputStream);
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

    private void checkSourcePath(Path sourcePath) {
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + sourcePath);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Path " + sourcePath + " expected to be a file");
        }
    }
}
