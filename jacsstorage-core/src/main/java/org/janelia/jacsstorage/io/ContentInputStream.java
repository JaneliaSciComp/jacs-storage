package org.janelia.jacsstorage.io;

import java.io.IOException;
import java.io.InputStream;

public class ContentInputStream extends InputStream {

    private final String contentEntryName;
    private final InputStream inputStream;

    public ContentInputStream(String contentEntryName, InputStream inputStream) {
        this.contentEntryName = contentEntryName;
        this.inputStream = inputStream;
    }

    @Override
    public int read() throws IOException {
        if (inputStream == null) {
            return -1;
        } else {
            return inputStream.read();
        }
    }
}
