package org.janelia.jacsstorage.io.contenthandlers;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.io.DataContentUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.inject.Vetoed;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class NoOPContentConverter implements ContentConverter {

    private static final Logger LOG = LoggerFactory.getLogger(NoOPContentConverter.class);

    private final boolean alwaysArchive;

    public NoOPContentConverter(boolean alwaysArchive) {
        this.alwaysArchive = alwaysArchive;
    }

    @Override
    public boolean support(String filterType) {
        return true;
    }

    @TimedMethod(
            argList = {0},
            logResult = true
    )
    @Override
    public long convertContent(DataContent dataContent, OutputStream outputStream) {
        if (alwaysArchive) {
            return archiveContent(dataContent, outputStream);
        } else {
            List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(2).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(peekDataNodes)) {
                return 0L;
            } else if (peekDataNodes.size() == 1) {
                InputStream dataStream = dataContent.streamDataNode(peekDataNodes.get(0));
                try {
                    return IOStreamUtils.copyFrom(dataStream, outputStream);
                } finally {
                    try {
                        dataStream.close();
                    } catch (Exception e) {
                        LOG.warn("Error closing data stream for {}", peekDataNodes.get(0), e);
                    }
                }
            } else {
                return archiveContent(dataContent, outputStream);
            }
        }
    }

    private long archiveContent(DataContent dataContent, OutputStream outputStream) {
        TarArchiveOutputStream archiveOutputStream = dataContent.streamDataNodes()
                .sorted(DataContentUtils.getDataNodePathComparator())
                .reduce(
                        new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE),
                        (archiveStream, dn) -> {
                            Path entryPath = Paths.get(dn.getNodeRelativePath());
                            String entryName;
                            long entrySize;
                            if (dn.isCollectionFlag()) {
                                entryName = StringUtils.appendIfMissing(
                                        StringUtils.prependIfMissing(
                                                StringUtils.prependIfMissing(entryPath.toString(), "/"), "."),
                                        "/"); // append '/' for directory entries
                                entrySize = 0L;
                            } else {
                                entryName = StringUtils.prependIfMissing(
                                        StringUtils.prependIfMissing(entryPath.toString(), "/"),
                                        ".");
                                entrySize = dn.getSize();
                            }
                            TarArchiveEntry entry = new TarArchiveEntry(entryName);
                            entry.setSize(entrySize);
                            try {
                                archiveStream.putArchiveEntry(entry);
                                if (!dn.isCollectionFlag()) {
                                    InputStream dataStream = dataContent.streamDataNode(dn);
                                    try {
                                        IOStreamUtils.copyFrom(dataStream, archiveStream);
                                    } finally {
                                        try {
                                            dataStream.close();
                                        } catch (Exception e) {
                                            LOG.warn("Error closing data stream for {}", dn, e);
                                        }
                                    }
                                }
                                archiveStream.closeArchiveEntry();
                                return archiveStream;
                            } catch (Exception e) {
                                LOG.error("Error copying data from {} for {}", dn.getNodeAccessURL(), dataContent, e);
                                throw new IllegalStateException(e);
                            }
                        },
                        (a1, a2) -> a1);

        try {
            archiveOutputStream.finish();
            long nbytesWritten = archiveOutputStream.getBytesWritten();
            LOG.info("Archived {} bytes", nbytesWritten);
            return nbytesWritten;
        } catch (IOException e) {
            LOG.error("Error ending the archive stream for {}", dataContent, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public long estimateContentSize(DataContent dataContent) {
        if (alwaysArchive) {
            return estimateArchivedContentSize(dataContent);
        } else {
            List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(2).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(peekDataNodes)) {
                return 0L;
            } else if (peekDataNodes.size() == 1) {
                return peekDataNodes.get(0).getSize();
            } else {
                return estimateArchivedContentSize(dataContent);
            }
        }
    }

    private long estimateArchivedContentSize(DataContent dataContent) {
        return dataContent.streamDataNodes()
                .sorted(DataContentUtils.getDataNodePathComparator())
                .reduce(
                        (long) 2 * TarConstants.DEFAULT_RCDSIZE,
                        (size, dn) -> {
                            long entrySize;
                            if (dn.isCollectionFlag()) {
                                entrySize = DataContentUtils.calculateTarEntrySize(0L);
                            } else {
                                entrySize = DataContentUtils.calculateTarEntrySize(dn.getSize());
                            }
                            return size + entrySize;
                        },
                        (s1, s2) -> s1 + s2);
    }
}
