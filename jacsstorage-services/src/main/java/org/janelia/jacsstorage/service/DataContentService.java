package org.janelia.jacsstorage.service;

import java.io.InputStream;

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
     * Read content from a single object.
     *
     * @param contentURI   contentURI
     * @return
     */
    ContentGetter getObjectContent(JADEStorageURI contentURI);

    /**
     * Read data from the specified URI and apply filter based on filterParams. This can read the content from one or multiple objects.
     *
     * @param contentURI   contentURI
     * @param contentAccessParams
     * @return
     */
    ContentGetter getDataContent(JADEStorageURI contentURI, ContentAccessParams contentAccessParams);

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
