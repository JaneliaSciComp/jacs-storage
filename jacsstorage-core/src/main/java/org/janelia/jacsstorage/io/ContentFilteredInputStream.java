package org.janelia.jacsstorage.io;

import java.io.IOException;
import java.io.InputStream;

public class ContentFilteredInputStream extends InputStream {

    private final ContentFilterParams contentFilterParams;
    private final InputStream inputStream;

    public ContentFilteredInputStream(ContentFilterParams contentFilterParams, InputStream inputStream) {
        this.contentFilterParams = contentFilterParams;
        this.inputStream = inputStream;
    }

    public ContentFilterParams getContentFilterParams() {
        return contentFilterParams;
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
