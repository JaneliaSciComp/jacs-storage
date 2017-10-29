package org.janelia.jacsstorage.io;

import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class TarArchiveBundleReader extends AbstractBundleReader {

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @Override
    protected long readBundleBytes(String source, OutputStream stream) throws Exception {
        Path sourcePath = getSourcePath(source);
        return Files.copy(sourcePath, stream);
    }

    @Override
    public List<DataNodeInfo> listBundleContent(String source, int depth) {
        Path sourcePath = getSourcePath(source);
        try {
            TarArchiveInputStream inputStream = new TarArchiveInputStream(new FileInputStream(sourcePath.toFile()));
            List<DataNodeInfo> dataNodeList = new ArrayList<>();
            for (ArchiveEntry sourceEntry = inputStream.getNextEntry(); sourceEntry != null; sourceEntry = inputStream.getNextEntry()) {
                String entryName = StringUtils.removeStart(sourceEntry.getName(), "./");
                int currentDepth = Splitter.on('/').omitEmptyStrings().splitToList(entryName).size();
                if (currentDepth > depth) {
                    continue;
                }
                DataNodeInfo dataNodeInfo = new DataNodeInfo();
                if (sourceEntry.isDirectory()) {
                    dataNodeInfo.setCollectionFlag(true);
                }
                dataNodeInfo.setNodePath(entryName);
                dataNodeInfo.setSize(sourceEntry.getSize());
                dataNodeInfo.setCreationTime(sourceEntry.getLastModifiedDate());
                dataNodeInfo.setLastModified(sourceEntry.getLastModifiedDate());
                dataNodeList.add(dataNodeInfo);
            }
            return dataNodeList;
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long readDataEntry(String source, String entryName, OutputStream outputStream) throws IOException {
        Path sourcePath = getSourcePath(source);
        if (StringUtils.isBlank(entryName)) {
            try {
                return readBundleBytes(source, outputStream);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        TarArchiveOutputStream tarOutputStream = null;
        long nbytes = 0L;
        TarArchiveInputStream inputStream = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(sourcePath.toFile())));
        try {
            String normalizedEntryName = normalizeEntryName(entryName);
            for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
                String currentEntryName = normalizeEntryName(sourceEntry.getName());
                if (currentEntryName.equals(normalizedEntryName)) {
                    if (sourceEntry.isDirectory()) {
                        tarOutputStream = new TarArchiveOutputStream(outputStream);
                    } else {
                        return ByteStreams.copy(ByteStreams.limit(inputStream, sourceEntry.getSize()), outputStream);
                    }
                }
                if (currentEntryName.startsWith(normalizedEntryName)) {
                    String relativeEntryName = StringUtils.removeStart(currentEntryName, normalizedEntryName);
                    String newEntryName = StringUtils.prependIfMissing(
                            StringUtils.prependIfMissing(relativeEntryName, "/"),
                            ".");
                    if (sourceEntry.isDirectory()) {
                        newEntryName = StringUtils.appendIfMissing(newEntryName, "/");
                    }
                    TarArchiveEntry entry = new TarArchiveEntry(newEntryName, true);
                    entry.setSize(sourceEntry.getSize());
                    entry.setModTime(sourceEntry.getModTime());
                    entry.setMode(sourceEntry.getMode());
                    tarOutputStream.putArchiveEntry(entry);
                    if (sourceEntry.isFile()) {
                        nbytes += ByteStreams.copy(ByteStreams.limit(inputStream, sourceEntry.getSize()), tarOutputStream);
                    }
                    tarOutputStream.closeArchiveEntry();
                } else if (tarOutputStream != null) {
                    tarOutputStream.finish();
                    return nbytes;
                }
            }
            if (tarOutputStream == null)
                throw new IllegalArgumentException("No entry " + normalizedEntryName + " found under " + source);
            else {
                tarOutputStream.finish();
                return nbytes;
            }
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    private Path getSourcePath(String source) {
        Path sourcePath = Paths.get(source);
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + source);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Source " + source + " expected to be a file");
        }
        return sourcePath;
    }

    private String normalizeEntryName(String name) {
        return StringUtils.removeEnd(
                StringUtils.removeStart(name, "./"),
                "/");
    }
}
