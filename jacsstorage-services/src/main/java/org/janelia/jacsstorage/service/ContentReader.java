package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.janelia.jacsstorage.io.ContentFilterParams;

public interface ContentReader {
    InputStream getContentInputstream();
}
