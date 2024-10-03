package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.jacsstorage.service.impl.contenthandling.ContentNodeHelper;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiffROIPixelsFilter implements ContentAccess {

    private static final Logger LOG = LoggerFactory.getLogger(TiffROIPixelsFilter.class);

    @Override
    public boolean isSupportedAccessType(String contentAccessType) {
        return "TIFF_ROI_PIXELS".equalsIgnoreCase(contentAccessType);
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes, ContentAccessParams filterParams, OutputStream outputStream) {
        Integer xCenter = filterParams.getAsInt("xCenter", 0);
        Integer yCenter = filterParams.getAsInt("yCenter", 0);
        Integer zCenter = filterParams.getAsInt("zCenter", 0);
        Integer dimX = filterParams.getAsInt("dimX", -1);
        Integer dimY = filterParams.getAsInt("dimY", -1);
        Integer dimZ = filterParams.getAsInt("dimZ", -1);

        try {
            if (CollectionUtils.isEmpty(contentNodes)) {
                return 0L;
            } else if (contentNodes.size() == 1) {
                try (InputStream nodeContent = contentNodes.get(0).getContent()) {
                    return IOStreamUtils.copyFrom(ImageUtils.loadImagePixelBytesFromTiffStream(
                            nodeContent,
                            xCenter, yCenter, zCenter,
                            dimX, dimY, dimZ
                    ), outputStream);
                }
            } else {
                TarArchiveOutputStream archiveOutputStream = new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE);
                String commonPrefix = ContentNodeHelper.commonPrefix(contentNodes);
                for (ContentNode contentNode : contentNodes) {
                    String tarEntryPrefix = commonPrefix.isEmpty() ? contentNode.getPrefix() : commonPrefix;
                    String tarEntryName = tarEntryPrefix.isEmpty() ? contentNode.getName() : tarEntryPrefix + "/" + contentNode.getName();
                    TarArchiveEntry entry = new TarArchiveEntry(tarEntryName);
                    entry.setSize(contentNode.getSize());
                    archiveOutputStream.putArchiveEntry(entry);
                    try (InputStream nodeContent = contentNode.getContent()) {
                        IOStreamUtils.copyFrom(ImageUtils.loadImagePixelBytesFromTiffStream(
                                nodeContent,
                                xCenter, yCenter, zCenter,
                                dimX, dimY, dimZ
                        ), archiveOutputStream);
                    }
                    archiveOutputStream.closeArchiveEntry();
                }
                archiveOutputStream.finish();
                long nbytesWritten = archiveOutputStream.getBytesWritten();
                LOG.info("Archived {} bytes", nbytesWritten);
                return nbytesWritten;
            }
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

}
