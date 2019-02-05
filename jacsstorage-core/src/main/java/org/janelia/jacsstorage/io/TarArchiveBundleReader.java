package org.janelia.jacsstorage.io;

import com.google.common.base.Splitter;
import com.google.common.io.ByteStreams;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.model.jacsstorage.JacsStorageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import java.util.Set;

public class TarArchiveBundleReader extends AbstractBundleReader {

    private final static Logger LOG = LoggerFactory.getLogger(TarArchiveBundleReader.class);

    @Inject
    TarArchiveBundleReader(ContentStreamFilterProvider contentStreamFilterProvider) {
        super(contentStreamFilterProvider);
    }

    @Override
    public Set<JacsStorageFormat> getSupportedFormats() {
        return EnumSet.of(JacsStorageFormat.ARCHIVE_DATA_FILE);
    }

    @TimedMethod(
            logResult = true
    )
    @Override
    public List<DataNodeInfo> listBundleContent(String source, String entryName, int depth) {
        Path sourcePath = getSourcePath(source);
        if (Files.notExists(sourcePath)) {
            return Collections.emptyList();
        }
        try {
            TarArchiveInputStream inputStream = new TarArchiveInputStream(new FileInputStream(sourcePath.toFile()));
            List<DataNodeInfo> dataNodeList = new ArrayList<>();
            String normalizedEntryName = normalizeEntryName(entryName);
            int normalizedEntryNameLength = normalizedEntryName.length();
            for (ArchiveEntry sourceEntry = inputStream.getNextEntry(); sourceEntry != null; sourceEntry = inputStream.getNextEntry()) {
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
                        dataNodeList.add(dataNodeInfo);
                        break;
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
                    }  else {
                        dataNodeInfo.setNodeRelativePath(currentEntryName);
                    }
                    dataNodeInfo.setSize(sourceEntry.getSize());
                    dataNodeInfo.setCreationTime(sourceEntry.getLastModifiedDate());
                    dataNodeInfo.setLastModified(sourceEntry.getLastModifiedDate());
                    dataNodeList.add(dataNodeInfo);
                }
            }
            return dataNodeList;
        } catch (Exception e) {
            throw new IllegalStateException(e);
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
        TarArchiveOutputStream tarOutputStream = null;
        long nbytes = 0L;
        TarArchiveInputStream inputStream = openSourceAsArchiveStream(sourcePath);
        try {
            ContentStreamFilter contentStreamFilter = contentStreamFilterProvider.getContentStreamFilter(filterParams);
            String normalizedEntryName = normalizeEntryName(entryName);
            for (TarArchiveEntry sourceEntry = inputStream.getNextTarEntry(); sourceEntry != null; sourceEntry = inputStream.getNextTarEntry()) {
                String currentEntryName = normalizeEntryName(sourceEntry.getName());
                if (currentEntryName.equals(normalizedEntryName)) {
                    if (sourceEntry.isDirectory()) {
                        tarOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
                    } else {
                        return IOStreamUtils.copyFrom(
                                contentStreamFilter.apply(new ContentFilteredInputStream(filterParams, ByteStreams.limit(inputStream, sourceEntry.getSize()))),
                                outputStream);
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
                    TarArchiveEntry entry = new TarArchiveEntry(newEntryName, false);
                    entry.setSize(sourceEntry.getSize());
                    entry.setModTime(sourceEntry.getModTime());
                    entry.setMode(sourceEntry.getMode());
                    tarOutputStream.putArchiveEntry(entry);
                    if (sourceEntry.isFile()) {
                        nbytes += IOStreamUtils.copyFrom(
                                contentStreamFilter.apply(new ContentFilteredInputStream(filterParams, ByteStreams.limit(inputStream, sourceEntry.getSize()))),
                                tarOutputStream);
                    }
                    tarOutputStream.closeArchiveEntry();
                }
            }
            if (tarOutputStream == null)
                throw new IllegalArgumentException("No entry " + normalizedEntryName + " found under " + source);
            else {
                tarOutputStream.finish();
                return nbytes;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignore) {
            }
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
}
