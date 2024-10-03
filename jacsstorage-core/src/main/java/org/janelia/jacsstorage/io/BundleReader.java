package org.janelia.jacsstorage.io;

import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public interface BundleReader {
    Set<JacsStorageFormat> getSupportedFormats();

    /**
     * Get content info
     * @param source
     * @param entryName
     * @return
     */
    Map<String, Object> getContentInfo(String source, String entryName);

    /**
     * List bundle content.
     * @param source bundle
     * @param entryName
     * @param depth
     * @return
     */
    Stream<DataNodeInfo> streamBundleContent(String source, String entryName, int depth);

    /**
     * Estimate the specified entry size from the bundle.
     * @param source
     * @param entryName
     * @param filterParams
     * @return
     */
    long estimateDataEntrySize(String source, String entryName, ContentAccessParams filterParams);

    /**
     * Read the specified entry from the bundle.
     * @param source
     * @param entryName
     * @param filterParams
     * @param outputStream
     * @return
     */
    long readDataEntry(String source, String entryName, ContentAccessParams filterParams, OutputStream outputStream);

    /**
     * Reads the data bundle from the specified source and writes it to the given output stream.
     *
     * @param source bundle
     * @param filterParams content filter parameters
     * @param stream to write the bytes formatted as a tar archive
     * @return the number of bytes transfered from source to the output stream
     */
    long readBundle(String source, ContentAccessParams filterParams, OutputStream stream);
}
