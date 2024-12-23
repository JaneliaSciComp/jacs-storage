package org.janelia.jacsstorage.service.n5;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.saalfeldlab.n5.universe.N5TreeNode;

/**
 * Service for reading and writing content to a specified storage URI
 */
public interface N5ContentService {
    /**
     * Check if the content identified by the specified URI exists.
     *
     * @param storageURI N5 container location
     * @return N5TreeNode
     */
    N5TreeNode getN5Container(JADEStorageURI storageURI);
}
