package org.janelia.jacsstorage.service;

import java.io.OutputStream;
import java.util.List;
import java.util.Map;

/**
 * This is used for getting and combining the content from multiple objects.
 * The instances of this interface must "know" how to access the content the enclosed objects.
 */
public interface ContentGetter {

    /**
     * Estimate content size of the enclosed nodes
     * @return size estimate
     */
    long estimateContentSize();

    /**
     * Accessor for the enclosed objects whose content is being retrieved.
     *
     * @return the list of objects
     */
    List<ContentNode> getObjectsList();

    /**
     * Retrieve content metadata.
     * @return
     */
    Map<String, Object> getMetaData();

    /**
     * Stream content of the enclosed nodes to the provided output
     *
     * @param outputStream result output stream
     * @return number of bytes
     */
    long streamContent(OutputStream outputStream);
}
