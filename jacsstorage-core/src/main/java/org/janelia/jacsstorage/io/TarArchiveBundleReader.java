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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    public InputStream readDataEntry(String source, String entryName) throws IOException {
        Path sourcePath = getSourcePath(source);
        TarArchiveInputStream inputStream = new TarArchiveInputStream(new FileInputStream(sourcePath.toFile()));
        if (StringUtils.isBlank(entryName)) {
            return inputStream;
        }
        TarArchiveOutputStream outputStream = null;
        Pipe pipe = null;
        for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
            String currentEntryName = StringUtils.removeStart(sourceEntry.getName(), "./");
            if (currentEntryName.equals(entryName)) {
                if (sourceEntry.isDirectory()) {
                    pipe = Pipe.open();
                    outputStream = new TarArchiveOutputStream(Channels.newOutputStream(pipe.sink()));
                } else {
                    return ByteStreams.limit(inputStream, sourceEntry.getSize());
                }
            }
            if (pipe != null && currentEntryName.startsWith(entryName)) {
                String newEntryName = StringUtils.prependIfMissing(StringUtils.removeStart(currentEntryName, entryName), "./");
                TarArchiveEntry entry = new TarArchiveEntry(newEntryName, false);
                entry.setSize(sourceEntry.getSize());
                entry.setModTime(sourceEntry.getModTime());
                entry.setMode(sourceEntry.getMode());
                entry.setUserId(sourceEntry.getLongUserId());
                entry.setGroupId(sourceEntry.getLongGroupId());
                entry.setUserName(sourceEntry.getUserName());
                entry.setGroupName(sourceEntry.getGroupName());
                outputStream.putArchiveEntry(entry);
                if (sourceEntry.isFile()) {
                    ByteStreams.copy(ByteStreams.limit(inputStream, sourceEntry.getSize()), outputStream);
                }
                outputStream.closeArchiveEntry();
            } else {
                if (pipe != null) {
                    outputStream.finish();;
                    pipe.sink().close();
                    return Channels.newInputStream(pipe.source());
                }
            }
        }
        if (pipe == null) {
            throw new IllegalArgumentException("No entry " + entryName + " found under " + source);
        } else {
            outputStream.finish();;
            pipe.sink().close();
            return Channels.newInputStream(pipe.source());
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

}
