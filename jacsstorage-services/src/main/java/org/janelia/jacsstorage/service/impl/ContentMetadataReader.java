package org.janelia.jacsstorage.service.impl;

import java.util.Map;

import org.janelia.jacsstorage.service.ContentNode;

/**
 * A filter is something that is applied to a list of nodes either to
 * extract and filter the content or extract the metadata.
 */
public interface ContentMetadataReader {
    /**
     * Return true if this filter can supports the requested mimetype.
     *
     * @param mimeType
     * @return
     */
    boolean support(String mimeType);

    /**
     *
     * @param filterParams
     * @param contentNode
     * @return
     */
    Map<String, Object> getMetadata(ContentNode contentNode);
}
