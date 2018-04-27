package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.msgpack.core.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class SingleFileBundleReader extends AbstractBundleReader {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @TimedMethod(
            argList = {0},
            logResult = true
    )
    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        return Files.copy(sourcePath, stream);
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
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        return ImmutableList.of(pathToDataNodeInfo(sourcePath, sourcePath, (rootPath, nodePath) -> rootPath.toString()));
    }

    @TimedMethod(
            argList = {0, 1},
            logResult = true
    )
    @Override
    public long readDataEntry(String source, String entryName, OutputStream outputStream) throws IOException {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        Preconditions.checkArgument(StringUtils.isBlank(entryName), "A single file (" + source + ") does not have any entry (" + entryName + ")");
        return Files.copy(sourcePath, outputStream);
    }

    private Path getSourcePath(String source) {
        return Paths.get(source);
    }

    private void checkSourcePath(Path sourcePath) {
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + sourcePath);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Path " + sourcePath + " expected to be a file");
        }
    }
}
