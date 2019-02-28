package org.janelia.jacsstorage.io;

import java.io.InputStream;
import java.util.Map;

public interface ContentInfoExtractor {
    boolean support(String mimeType);
    Map<String, Object> extractContentInfo(InputStream inputStream);
}
