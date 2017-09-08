package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.OutputStream;
import java.util.Set;

public interface BundleReader {
    Set<JacsStorageFormat> getSupportedFormats();

    /**
     * Reads the data bundle from the specified source and writes it to the given output stream.
     *
     * @param source bundle
     * @param stream to write the bytes formatted as a tar archive
     * @return data transfer info
     */
    TransferInfo readBundle(String source, OutputStream stream);
}
