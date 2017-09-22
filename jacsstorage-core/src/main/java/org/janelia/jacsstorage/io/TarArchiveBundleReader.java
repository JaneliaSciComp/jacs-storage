package org.janelia.jacsstorage.io;

import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.EnumSet;
import java.util.Set;

public class TarArchiveBundleReader extends AbstractBundleReader {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        Path sourcePath = Paths.get(source);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        } else {
            return Files.copy(sourcePath, stream);
        }
    }

}
