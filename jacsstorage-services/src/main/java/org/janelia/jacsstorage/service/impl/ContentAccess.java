package org.janelia.jacsstorage.service.impl;

import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;

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
    boolean isAccessTypeSupported(String contentAccessType);

    /**
     * Estimate content size
     * @param contentNodes
     * @param contentAccessParams
     * @param contentObjectReader
     * @return
     */
    long estimateContentSize(List<ContentNode> contentNodes, ContentAccessParams contentAccessParams, ContentStreamReader contentObjectReader);

    /**
     * Apply filter to the give nodes.
     *
     * @param contentNodes list of nodes from which the content is extracted
     * @param contentAccessParams content access specific paremeters.
     * @param contentObjectReader object that knows how to stream content for a single node.
     * @param outputStream result output stream
     * @return
     */
    long retrieveContent(List<ContentNode> contentNodes, ContentAccessParams contentAccessParams,
                         ContentStreamReader contentObjectReader,
                         OutputStream outputStream);
}
