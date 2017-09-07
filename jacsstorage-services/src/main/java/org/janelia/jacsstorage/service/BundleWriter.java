package org.janelia.jacsstorage.service;

import java.io.InputStream;

public interface BundleWriter {
    /**
     * Reads the bytes from the given stream and writes it to the specified target.
     *
     * @param stream a tar archive stream of bytes to be written
     * @param target bundle
     * @return data transfer info
     */
    TransferInfo writeBundle(InputStream stream, String target);
}
