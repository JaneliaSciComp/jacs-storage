package org.janelia.jacsstorage.service;

import java.io.OutputStream;

public interface BundleReader {
    /**
     * Reads the data bundle from the specified source and writes it to the given output stream.
     *
     * @param source bundle
     * @param stream to write the bytes formatted as a tar archive
     * @return data transfer info
     */
    TransferInfo readBundle(String source, OutputStream stream);
}
