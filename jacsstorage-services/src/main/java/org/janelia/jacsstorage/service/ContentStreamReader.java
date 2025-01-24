package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.io.OutputStream;

public interface ContentStreamReader {
    InputStream getContentInputStream(String contentLocation);

    long streamContentToOutput(String contentLocation, OutputStream outputStream);
}
