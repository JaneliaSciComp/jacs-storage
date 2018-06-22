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
     * @return number of bytes transfered from the input stream to the target
     */
    long writeBundle(InputStream stream, String target);

    /**
     * Create a directory entry if of course the reader supports such operation. The precondition is that the parent entry
     * of the entryName must already exist and it must also be a directory.
     * @param dataPath
     * @param entryName - this is the relative path of the new entry.
     * @return - the additional space required by the new directory entry
     */
    long createDirectoryEntry(String dataPath, String entryName);

    /**
     * Create a file entry and copy the content. The parent entry must exist and must be a directory.
     * @param dataPath
     * @param entryName
     * @param contentStream
     * @return the additional space required by the new file entry
     */
    long createFileEntry(String dataPath, String entryName, InputStream contentStream);
}
