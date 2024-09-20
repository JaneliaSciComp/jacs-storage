package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentNode;

public interface ContentFilter {
    boolean support(String filterType);
    long applyContentFilter(ContentFilterParams filterParams,
                            List<ContentNode> contentNodes,
                            OutputStream outputStream);
}
