package org.janelia.jacsstorage.service.impl;

import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentNode;

/**
 * A filter is something that is applied to a list of nodes to
 * extract and/or combine the content from the given nodes.
 */
public interface ContentAccess {
    /**
     * Return true if this filter can supports the requested type.
     *
     * @param contentAccessType
     * @return
     */
    boolean isSupportedAccessType(String contentAccessType);

    /**
     * Apply filter to the give nodes.
     *
     * @param contentNodes list of nodes from which the content is extracted
     * @param filterParams filter specific paremeters.
     * @param outputStream result output stream
     * @return
     */
    long retrieveContent(List<ContentNode> contentNodes, ContentAccessParams filterParams, OutputStream outputStream);
}
