package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.EnumSet;
import java.util.Set;

public class SingleFileBundleReader extends AbstractBundleReader {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.SINGLE_DATA_FILE);
    }

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        File sourcePath = new File(source);
        if (!sourcePath.exists()) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!sourcePath.isFile()) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        }
        long nBytes = Files.copy(sourcePath.toPath(), stream);
        return nBytes;
    }

}
