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
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Vetoed
public class NoOPContentConverter implements ContentConverter {

    private static final Logger LOG = LoggerFactory.getLogger(NoOPContentConverter.class);

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
        List<DataNodeInfo> dataNodes = dataContent.listDataNodes();
        if (CollectionUtils.isEmpty(dataNodes)) {
            return 0L;
        } else if (dataNodes.size() == 1) {
            if (!dataNodes.get(0).isCollectionFlag()) {
                return IOStreamUtils.copyFrom(dataContent.streamDataNode(dataNodes.get(0)), outputStream);
            } else {
                return 0L;
            }
        } else {
            Pair<TarArchiveOutputStream, Long> streamWithLength = dataNodes.stream()
                    .sorted(DataContentUtils.getDataNodePathComparator())
                    .reduce(
                            Pair.of(new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE), 0L),
                            (p, dn) -> {
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
                                long nbytes;
                                try {
                                    p.getLeft().putArchiveEntry(entry);
                                    if (!dn.isCollectionFlag()) {
                                        nbytes = IOStreamUtils.copyFrom(dataContent.streamDataNode(dn), p.getLeft());
                                    } else {
                                        nbytes = 0L;
                                    }
                                    p.getLeft().closeArchiveEntry();
                                    return Pair.of(p.getLeft(), p.getRight() + nbytes);
                                } catch (Exception e) {
                                    LOG.error("Error copying data from {} for {}", dn.getNodeAccessURL(), dataContent, e);
                                    throw new IllegalStateException(e);
                                }
                            },
                            (p1, p2) -> Pair.of(p2.getLeft(), p1.getRight() + p2.getRight()));

            try {
                streamWithLength.getLeft().finish();
            } catch (IOException e) {
                LOG.error("Error ending the archive stream for {}", dataContent, e);
                throw new IllegalStateException(e);
            }
            return streamWithLength.getRight();
        }
    }
}