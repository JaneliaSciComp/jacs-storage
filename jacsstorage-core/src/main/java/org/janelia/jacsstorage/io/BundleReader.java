package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.OutputStream;
import java.util.List;
import java.util.Set;

public interface BundleReader {
    Set<JacsStorageFormat> getSupportedFormats();

    /**
     * List bundle content.
     * @param source bundle
     * @param entryName
     * @param depth
     * @return
     */
    List<DataNodeInfo> listBundleContent(String source, String entryName, int depth);

    /**
     * Read the specified entry from the bundle.
     * @param source
     * @param entryName
     * @param filterParams
     * @param outputStream
     * @return
     */
    long readDataEntry(String source, String entryName, ContentFilterParams filterParams, OutputStream outputStream);

    /**
     * Reads the data bundle from the specified source and writes it to the given output stream.
     *
     * @param source bundle
     * @param filterParams content filter parameters
     * @param stream to write the bytes formatted as a tar archive
     * @return the number of bytes transfered from source to the output stream
     */
    long readBundle(String source, ContentFilterParams filterParams, OutputStream stream);
}
