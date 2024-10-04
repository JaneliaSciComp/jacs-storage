package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentAccessParams;

public interface ContentStreamReader {
    InputStream readContent(String contentLocation);
}
