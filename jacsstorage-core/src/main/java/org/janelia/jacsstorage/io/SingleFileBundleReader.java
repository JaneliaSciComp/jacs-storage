package org.janelia.jacsstorage.io;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class SingleFileBundleReader extends AbstractBundleReader {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        Path sourcePath = getSourcePath(source);
        return Files.copy(sourcePath, stream);
    }

    @Override
    public List<DataNodeInfo> listBundleContent(String source, int depth) {
        Path sourcePath = getSourcePath(source);
        return ImmutableList.of(pathToDataNodeInfo(sourcePath, sourcePath));
    }

    @Override
    public InputStream readDataEntry(String source, String entryName) throws IOException {
        Path sourcePath = getSourcePath(source);
        if (StringUtils.isNotBlank(entryName)) {
            throw new IllegalArgumentException("A single file (" + source + ") does not have any entry (" + entryName + ")");
        }
        return new FileInputStream(sourcePath.toFile());
    }

    private Path getSourcePath(String source) {
        Path sourcePath = Paths.get(source);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        }
        return sourcePath;
    }

}
