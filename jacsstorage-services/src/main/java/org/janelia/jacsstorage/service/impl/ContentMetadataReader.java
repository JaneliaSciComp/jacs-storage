package org.janelia.jacsstorage.service.impl;

import java.util.Map;

import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;

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
     * @param contentNode
     * @param contentObjectReader object's content reader in case it's needed
     * @return
     */
    Map<String, Object> getMetadata(ContentNode contentNode, ContentStreamReader contentObjectReader);
}
