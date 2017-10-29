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
    public InputStream readDataEntry(String source, String entryName) throws IOException {
        Path sourcePath = getSourcePath(source);
        TarArchiveInputStream inputStream = new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(sourcePath.toFile())));
        if (StringUtils.isBlank(entryName)) {
            return inputStream;
        }
        String normalizedEntryName = normalizeEntryName(entryName);
        for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
            String currentEntryName = normalizeEntryName(sourceEntry.getName());
            if (currentEntryName.equals(normalizedEntryName)) {
                if (sourceEntry.isDirectory()) {
                    TarArchiveEntry currentEntry = sourceEntry;
                    ByteBuffer readBuffer = ByteBuffer.allocate(512);
                    readBuffer.limit(0);
                    Pipe pipe = Pipe.open();
                    pipe.source().configureBlocking(true);

                    return new InputStream() {
                        TarArchiveEntry nextStreamedEntry = currentEntry;

                        TarArchiveOutputStream outputStream = new TarArchiveOutputStream(Channels.newOutputStream(pipe.sink()));
                        InputStream entryContentStream = null;
                        TarStreamerState writerState = TarStreamerState.WRITE_ENTRYHEADER;

                        @Override
                        public int read() throws IOException {
                            if (!readBuffer.hasRemaining()) {
                                if (!fillBuffer()) {
                                    return -1;
                                }
                            }
                            return readBuffer.get();
                        }

                        private boolean fillBuffer() throws IOException {
                            boolean endLoop = false;
                            while (!endLoop) {
                                switch (writerState) {
                                    case WRITE_ENTRYHEADER:
                                        endLoop = writeEntryHeader();
                                        break;
                                    case WRITE_ENTRYCONTENT:
                                        endLoop = writeEntryContent();
                                        break;
                                    case WRITE_ENTRYFOOTER:
                                        endLoop = writeEntryFooter();
                                        break;
                                    case WRITE_COMPLETED:
                                        pipe.sink().close();
                                        endLoop = true;
                                        break;
                                    default:
                                        return false;
                                }
                            }
                            int nbytes = fillReaderBuffer();
                            if (nbytes == -1) {
                                pipe.source().close();
                                return false;
                            } else {
                                return true;
                            }
                        }

                        private int fillReaderBuffer() throws IOException {
                            readBuffer.clear();
                            int nbytes = pipe.source().read(readBuffer);
                            if (nbytes == -1) {
                                readBuffer.limit(0);
                            } else if (nbytes == 0) {
                                readBuffer.limit(0);
                            } else {
                                readBuffer.flip();
                            }
                            return nbytes;
                        }

                        private boolean writeEntryHeader() throws IOException {
                            String currentEntryName = normalizeEntryName(nextStreamedEntry.getName());
                            String relativeEntryName = StringUtils.removeStart(currentEntryName, normalizedEntryName);
                            String newEntryName = StringUtils.prependIfMissing(
                                    StringUtils.prependIfMissing(normalizeEntryName(relativeEntryName), "/"),
                                    ".");
                            TarArchiveEntry entry = new TarArchiveEntry(newEntryName, false);
                            entry.setSize(nextStreamedEntry.getSize());
                            entry.setModTime(nextStreamedEntry.getModTime());
                            entry.setMode(nextStreamedEntry.getMode());
                            entry.setUserId(nextStreamedEntry.getLongUserId());
                            entry.setGroupId(nextStreamedEntry.getLongGroupId());
                            entry.setUserName(nextStreamedEntry.getUserName());
                            entry.setGroupName(nextStreamedEntry.getGroupName());
                            outputStream.putArchiveEntry(entry);
                            if (nextStreamedEntry.isFile()) {
                                writerState = TarStreamerState.WRITE_ENTRYCONTENT;
                                entryContentStream = ByteStreams.limit(inputStream, nextStreamedEntry.getSize());
                            } else {
                                writerState = TarStreamerState.WRITE_ENTRYFOOTER;
                            }
                            return true;
                        }

                        private boolean writeEntryContent() throws IOException {
                            int bufferLength = 512;
                            byte[] contentBuffer = new byte[bufferLength];
                            int nbytes = entryContentStream.read(contentBuffer);
                            if (nbytes == -1) {
                                writerState = TarStreamerState.WRITE_ENTRYFOOTER;
                                return false;
                            } else if (nbytes == 0) {
                                return false;
                            } else {
                                outputStream.write(contentBuffer, 0, nbytes);
                                writerState = TarStreamerState.WRITE_ENTRYCONTENT;
                                return nbytes == bufferLength;
                            }
                        }

                        private boolean writeEntryFooter() throws IOException {
                            outputStream.closeArchiveEntry();
                            nextStreamedEntry = inputStream.getNextTarEntry();
                            if (nextStreamedEntry == null) {
                                outputStream.finish();
                                writerState = TarStreamerState.WRITE_COMPLETED;
                            } else {
                                String currentEntryName = normalizeEntryName(nextStreamedEntry.getName());
                                if (currentEntryName.startsWith(normalizedEntryName)) {
                                    writerState = TarStreamerState.WRITE_ENTRYHEADER;
                                } else {
                                    outputStream.finish();
                                    writerState = TarStreamerState.WRITE_COMPLETED;
                                }
                            }
                            return false;
                        }

                        @Override
                        public void close() throws IOException {
                            pipe.source().close();
                        }
                    };
                } else {
                    return ByteStreams.limit(inputStream, sourceEntry.getSize());
                }
            }
        }
        throw new IllegalArgumentException("No entry " + normalizedEntryName + " found under " + source);
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
