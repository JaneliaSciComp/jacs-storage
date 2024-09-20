package org.janelia.jacsstorage.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileContentReader implements ContentReader {

    private final Logger LOG = LoggerFactory.getLogger(FileContentReader.class);

    private final Path filePath;

    FileContentReader(Path filePath) {
        this.filePath = filePath;
    }

    @Override
    public InputStream getContentInputstream() {
        try {
            LOG.trace("Read content from {}", filePath);
            return Files.newInputStream(filePath);
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }
}
