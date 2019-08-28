package org.janelia.jacsstorage.io;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TarArchiveBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(TarArchiveBundleReader.class);

    @Inject
    TarArchiveBundleReader(ContentHandlerProvider contentHandlerProvider) {
        super(contentHandlerProvider);
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Map<String, Object> getContentInfo(String source, String entryName) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        TarArchiveInputStream inputStream = openSourceAsArchiveStream(sourcePath);
        try {
            String normalizedEntryName = normalizeEntryName(entryName);
            for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
                String currentEntryName = normalizeEntryName(sourceEntry.getName());
                if (currentEntryName.equals(normalizedEntryName)) {
                    if (sourceEntry.isDirectory()) {
                        return ImmutableMap.of("collectionFlag", true);
                    } else {
                        ContentInfoExtractor contentInfoExtractor = contentHandlerProvider.getContentInfoExtractor(getMimeType(currentEntryName));
                        return contentInfoExtractor.extractContentInfo(ByteStreams.limit(inputStream, sourceEntry.getSize()));
                    }
                }
            }
            return ImmutableMap.of();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
        }
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public Stream<DataNodeInfo> streamBundleContent(String source, String entryName, int depth) {
        Path sourcePath = getSourcePath(source);
        if (Files.notExists(sourcePath)) {
            return Stream.of();
        }
        Spliterator<DataNodeInfo> dataNodeSupplier = new Spliterator<DataNodeInfo>() {
            String normalizedEntryName = normalizeEntryName(entryName);
            int normalizedEntryNameLength = normalizedEntryName.length();
            TarArchiveInputStream inputStream;

            @Override
            public boolean tryAdvance(Consumer<? super DataNodeInfo> action) {
                if (inputStream == null) {
                    try {
                        inputStream = new TarArchiveInputStream(new FileInputStream(sourcePath.toFile()));
                    } catch (IOException e) {
                        LOG.error("Error opening entry {} from {} ({})", entryName, source, sourcePath, e);
                        throw new IllegalStateException(e);
                    }
                }
                for(;;) {
                    ArchiveEntry sourceEntry;
                    try {
                        sourceEntry = inputStream.getNextEntry();
                    } catch (Exception e) {
                        LOG.error("Error getting the next node from {} at {} ({})", entryName, source, sourcePath);
                        try {
                            inputStream.close();
                        } catch (IOException ignore) {
                        } finally {
                            inputStream = null;
                        }
                        throw new IllegalStateException(e);
                    }
                    if (sourceEntry == null) {
                        // reached the end
                        try {
                            inputStream.close();
                        } catch (IOException ignore) {
                        } finally {
                            inputStream = null;
                        }
                        return false;
                    }
                    String currentEntryName = normalizeEntryName(sourceEntry.getName());
                    if (currentEntryName.equals(normalizedEntryName)) {
                        if (!sourceEntry.isDirectory()) {
                            DataNodeInfo dataNodeInfo = new DataNodeInfo();
                            dataNodeInfo.setStorageRootLocation(sourcePath.toString());
                            dataNodeInfo.setCollectionFlag(false);
                            dataNodeInfo.setNodeRelativePath(currentEntryName);
                            dataNodeInfo.setSize(sourceEntry.getSize());
                            dataNodeInfo.setCreationTime(sourceEntry.getLastModifiedDate());
                            dataNodeInfo.setLastModified(sourceEntry.getLastModifiedDate());
                            action.accept(dataNodeInfo);
                            return true;
                        }
                    }
                    if (currentEntryName.startsWith(normalizedEntryName)) {
                        int currentDepth = Splitter.on('/').omitEmptyStrings().splitToList(currentEntryName.substring(normalizedEntryNameLength)).size();
                        if (currentDepth > depth) {
                            continue;
                        }
                        DataNodeInfo dataNodeInfo = new DataNodeInfo();
                        if (sourceEntry.isDirectory()) {
                            dataNodeInfo.setCollectionFlag(true);
                            dataNodeInfo.setNodeRelativePath(StringUtils.appendIfMissing(currentEntryName, "/"));
                        } else {
                            dataNodeInfo.setNodeRelativePath(currentEntryName);
                        }
                        dataNodeInfo.setSize(sourceEntry.getSize());
                        dataNodeInfo.setCreationTime(sourceEntry.getLastModifiedDate());
                        dataNodeInfo.setLastModified(sourceEntry.getLastModifiedDate());
                        action.accept(dataNodeInfo);
                    }
                }
            }

            @Override
            public Spliterator<DataNodeInfo> trySplit() {
                return null; // not supported
            }

            @Override
            public long estimateSize() {
                return 0;
            }

            @Override
            public int characteristics() {
                return ORDERED;
            }
        };
        return StreamSupport.stream(dataNodeSupplier, false);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public long estimateDataEntrySize(String source, String entryName, ContentFilterParams filterParams) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        long size = 0L;
        try (TarArchiveInputStream inputStream = openSourceAsArchiveStream(sourcePath)) {
            String normalizedEntryName = normalizeEntryName(entryName);
            for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
                String currentEntryName = normalizeEntryName(sourceEntry.getName());
                if (currentEntryName.equals(normalizedEntryName)) {
                    if (!sourceEntry.isDirectory()) {
                        // if the entry is not a directory just stream it right away
                        if (filterParams.matchEntry(entryName)) {
                            return sourceEntry.getSize();
                        } else {
                            return 0;
                        }
                    }
                }
                if (currentEntryName.startsWith(normalizedEntryName) && !sourceEntry.isDirectory() && filterParams.matchEntry(currentEntryName)) {
                    size += sourceEntry.getSize();
                }
            }
            return size;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

    }

    @TimedMethod(
            argList = {0, 1, 2},
            logResult = true
    )
    @Override
    public long readDataEntry(String source, String entryName, ContentFilterParams filterParams, OutputStream outputStream) {
        Path sourcePath = getSourcePath(source);
        checkSourcePath(sourcePath);
        try (TarArchiveInputStream inputStream = openSourceAsArchiveStream(sourcePath)) {
            ContentConverter contentConverter = contentHandlerProvider.getContentConverter(filterParams);
            String normalizedEntryName = normalizeEntryName(entryName);
            ArchiveEntryListDataContent archiveEntryListDataContent = new ArchiveEntryListDataContent(filterParams, prepareEntryName(normalizedEntryName));
            for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
                String currentEntryName = normalizeEntryName(sourceEntry.getName());
                if (currentEntryName.equals(normalizedEntryName)) {
                    if (!sourceEntry.isDirectory()) {
                        // if the entry is not a directory just stream it right away
                        if (filterParams.matchEntry(entryName)) {
                            return contentConverter.convertContent(new SingleArchiveEntryDataContent(filterParams, prepareEntryName(currentEntryName), sourceEntry.getSize(), ByteStreams.limit(inputStream, sourceEntry.getSize())), outputStream);
                        } else {
                            return 0;
                        }
                    }
                }
                if (currentEntryName.startsWith(normalizedEntryName) && (sourceEntry.isDirectory() || filterParams.matchEntry(currentEntryName))) {
                    archiveEntryListDataContent.addArchiveEntry(prepareEntryName(currentEntryName), sourceEntry.isDirectory(), sourceEntry.isDirectory() ? 0L : sourceEntry.getSize(), inputStream);
                }
            }
            if (archiveEntryListDataContent.getEntriesCount() == 0) {
                throw new IllegalArgumentException("No entry " + normalizedEntryName + " found under " + source);
            } else {
                return contentConverter.convertContent(archiveEntryListDataContent, outputStream);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path getSourcePath(String source) {
        return Paths.get(source);
    }

    private void checkSourcePath(Path sourcePath) {
        if (Files.notExists(sourcePath)) {
            throw new IllegalArgumentException("No file found for " + sourcePath);
        } else if (!Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("Path " + sourcePath + " expected to be a file");
        }
    }

    private TarArchiveInputStream openSourceAsArchiveStream(Path sourcePath) {
        try {
            return new TarArchiveInputStream(new BufferedInputStream(new FileInputStream(sourcePath.toFile())));
        } catch (IOException e) {
            LOG.error("Error opening tar archive {}", sourcePath, e);
            throw new IllegalArgumentException(e);
        }
    }

    private String normalizeEntryName(String name) {
        if (StringUtils.isBlank(name)) return "";
        return StringUtils.removeEnd(
                StringUtils.removeStart(
                        StringUtils.removeStart(name, "."),
                        "/"),
                "/");
    }

    private String prepareEntryName(String name) {
        return "./" + name.replace(File.separatorChar, '/');
    }
}
