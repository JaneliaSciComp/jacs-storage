package org.janelia.jacsstorage.service;

import java.io.InputStream;

public interface ContentStreamReader {
    InputStream readContent(String contentLocation);
}
