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
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Vetoed
public class DirectContentAccess implements ContentAccess {

    private static final Logger LOG = LoggerFactory.getLogger(DirectContentAccess.class);

    private final boolean alwaysArchive;

    public DirectContentAccess(boolean alwaysArchive) {
        this.alwaysArchive = alwaysArchive;
    }

    DirectContentAccess() {
        this(false);
    }

    @Override
    public boolean isAccessTypeSupported(String contentAccessType) {
        return true;
    }

    @Override
    public long estimateContentSize(List<ContentNode> contentNodes, ContentAccessParams contentAccessParams, ContentStreamReader contentObjectReader) {
        if (alwaysArchive || contentNodes.size() > 1) {
            return estimateArchiveSize(contentNodes);
        } else {
            if (contentNodes.isEmpty()) {
                return 0;
            } else { // contentNodes.size() == 1
                return contentNodes.get(0).getSize();
            }
        }
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes,
                                ContentAccessParams contentAccessParams,
                                ContentStreamReader contentObjectReader,
                                OutputStream outputStream) {
        if (contentNodes.isEmpty()) {
            return 0;
        } else if (alwaysArchive || contentNodes.size() > 1) {
            return archiveContent(contentNodes, contentObjectReader, outputStream);
        } else { // contentNodes.size() == 1
            try (InputStream nodeContentStream = contentObjectReader.readContent(contentNodes.get(0).getObjectKey())) {
                return IOStreamUtils.copyFrom(nodeContentStream, outputStream);
            } catch (IOException e) {
                throw new ContentException(e);
            }
        }
    }

    private long archiveContent(List<ContentNode> contentNodes, ContentStreamReader contentObjectReader, OutputStream outputStream) {
        TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
        try {
            String commonPrefix = ContentNodeHelper.commonPrefix(contentNodes);
            for (ContentNode contentNode : contentNodes) {
                String entryPrefix = commonPrefix.isEmpty() ? contentNode.getPrefix() : commonPrefix;
                String entryName = entryPrefix.isEmpty() ? contentNode.getName() : entryPrefix + "/" + contentNode.getName();
                TarArchiveEntry entry = new TarArchiveEntry(entryName);
                entry.setSize(contentNode.getSize());
                archiveOutputStream.putArchiveEntry(entry);
                try (InputStream nodeContent = contentObjectReader.readContent(contentNode.getObjectKey())) {
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

    private long estimateArchiveSize(List<ContentNode> contentNodes) {
        long totalSize = 2 * TarConstants.DEFAULT_RCDSIZE;
        for (ContentNode contentNode : contentNodes) {
            long entrySize = contentNode.getSize();
            totalSize += ContentNodeHelper.calculateTarEntrySize(entrySize);
        }
        return totalSize;
    }

}
