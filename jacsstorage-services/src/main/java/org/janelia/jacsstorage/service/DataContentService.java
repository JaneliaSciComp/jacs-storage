package org.janelia.jacsstorage.service;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;

/**
 * Service for reading and writing content to a specified storage URI
 */
public interface DataContentService {
    /**
     * List content nodes from the specified URI.
     *
     * @param storageURI
     * @param filterParams
     * @return
     */
    List<ContentNode> listDataNodes(JADEStorageURI storageURI, ContentFilterParams filterParams);

    /**
     * Read data from the specified URI and apply filter based on filterParams.
     *
     * @param contentURI contentURI
     * @param filterParams
     * @param dataStream
     * @return
     */
    long readDataStream(JADEStorageURI contentURI, ContentFilterParams filterParams, OutputStream dataStream);

    /**
     * Write data at the specified URI
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
