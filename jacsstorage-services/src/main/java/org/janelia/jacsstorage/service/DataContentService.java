package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;

/**
 * Service for reading and writing content to a specified storage URI
 */
public interface DataContentService {
    /**
     * Check if the content identified by the specified URI exists.
     *
     * @param storageURI
     * @return
     */
    boolean exists(JADEStorageURI storageURI);

    /**
     * Compute storage capacity.
     *
     * @param storageURI
     * @return
     */
    StorageCapacity storageCapacity(JADEStorageURI storageURI);

    /**
     * List content nodes from the specified URI.
     *
     * @param storageURI
     * @param filterParams
     * @return
     */
    List<ContentNode> listDataNodes(JADEStorageURI storageURI, ContentFilterParams filterParams);

    /**
     * Read content's node metadata - a map of attributes that depends on the content.
     *
     * @param storageURI
     * @return
     */
    Map<String, Object> readNodeMetadata(JADEStorageURI storageURI);

    /**
     * Read data from the specified URI and apply filter based on filterParams.
     *
     * @param contentURI   contentURI
     * @param filterParams
     * @param dataStream
     * @return
     */
    long readDataStream(JADEStorageURI contentURI, ContentFilterParams filterParams, OutputStream dataStream);

    /**
     * Write data at the specified URI
     *
     * @param contentURI
     * @param dataStream
     * @return
     */
    long writeDataStream(JADEStorageURI contentURI, InputStream dataStream);

    /**
     * List content nodes from the specified URI.
     *
     * @param storageURI
     */
    void removeData(JADEStorageURI storageURI);
}
