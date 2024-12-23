package org.janelia.jacsstorage.service.n5;

import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.saalfeldlab.n5.N5Reader;

public interface N5ReaderProvider {
    N5Reader getN5Reader(JADEStorageURI storageURI);
}
