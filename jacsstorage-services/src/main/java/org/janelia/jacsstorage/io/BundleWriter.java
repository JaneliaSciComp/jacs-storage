package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.InputStream;
import java.util.Set;

public interface BundleWriter {
    Set<JacsStorageFormat> getSupportedFormats();

    /**
     * Reads the bytes from the given stream and writes it to the specified target.
     *
     * @param stream a tar archive stream of bytes to be written
     * @param target bundle
     * @return data transfer info
     */
    TransferInfo writeBundle(InputStream stream, String target);
}
