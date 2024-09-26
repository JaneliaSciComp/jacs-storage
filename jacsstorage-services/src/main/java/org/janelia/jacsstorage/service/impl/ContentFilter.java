package org.janelia.jacsstorage.service.impl;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentNode;

/**
 * A filter is something that is applied to a list of nodes to
 * extract and/or combine the content from the given nodes.
 */
public interface ContentFilter {
    /**
     * Return true if this filter can supports the requested type.
     *
     * @param filterType
     * @return
     */
    boolean support(String filterType);

    /**
     * Apply filter to the give nodes.
     *
     * @param filterParams filter specific paremeters.
     * @param contentNodes list of nodes from which the content is extracted
     * @param outputStream result output stream
     * @return
     */
    long applyContentFilter(ContentFilterParams filterParams,
                            List<ContentNode> contentNodes,
                            OutputStream outputStream);
}
