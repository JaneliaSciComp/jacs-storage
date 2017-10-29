package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
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

    /**
     * List bundle content.
     * @param source bundle
     * @param depth
     * @return
     */
    List<DataNodeInfo> listBundleContent(String source, int depth);

    /**
     * Read the specified entry from the bundle.
     * @param source
     * @param entryName
     * @param outputStream
     * @return
     * @throws IOException
     */
    long readDataEntry(String source, String entryName, OutputStream outputStream) throws IOException;
}
