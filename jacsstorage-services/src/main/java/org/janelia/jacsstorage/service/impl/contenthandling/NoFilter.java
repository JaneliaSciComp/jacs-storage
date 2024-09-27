package org.janelia.jacsstorage.service.impl.contenthandling;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.enterprise.inject.Vetoed;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Vetoed
public class NoFilter implements ContentFilter {

    private static final Logger LOG = LoggerFactory.getLogger(NoFilter.class);

    private final boolean alwaysArchive;

    public NoFilter(boolean alwaysArchive) {
        this.alwaysArchive = alwaysArchive;
    }

    NoFilter() {
        this(false);
    }

    @Override
    public boolean support(String filterType) {
        return true;
    }

    @Override
    public long applyContentFilter(ContentFilterParams filterParams, List<ContentNode> contentNodes, OutputStream outputStream) {
        if (alwaysArchive) {
            return archiveContent(contentNodes, outputStream);
        } else {
            if (contentNodes.isEmpty()) {
                return 0;
            } else if (contentNodes.size() == 1) {
                try (InputStream nodeContentStream = contentNodes.get(0).getContent()) {
                    return IOStreamUtils.copyFrom(nodeContentStream, outputStream);
                } catch (IOException e) {
                    throw new ContentException(e);
                }
            }
        }
        return 0;
    }

    private long archiveContent(List<ContentNode> contentNodes, OutputStream outputStream) {
        TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
        try {
            String commonPrefix = ContentNodeHelper.commonPrefix(contentNodes);
            for (ContentNode contentNode : contentNodes) {
                String entryPrefix = commonPrefix.isEmpty() ? contentNode.getPrefix() : commonPrefix;
                String entryName = entryPrefix.isEmpty() ? contentNode.getName() : entryPrefix + "/" + contentNode.getName();
                TarArchiveEntry entry = new TarArchiveEntry(entryName);
                entry.setSize(contentNode.getSize());
                archiveOutputStream.putArchiveEntry(entry);
                try (InputStream nodeContent = contentNode.getContent()) {
                    IOStreamUtils.copyFrom(nodeContent, archiveOutputStream);
                }
                archiveOutputStream.closeArchiveEntry();
            }
            archiveOutputStream.finish();
            long nbytesWritten = archiveOutputStream.getBytesWritten();
            LOG.info("Archived {} bytes", nbytesWritten);
            return nbytesWritten;
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }
}
