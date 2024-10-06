package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.jacsstorage.service.impl.contenthandling.ContentNodeHelper;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This object retrieves only the specified ROI from the enclosed nodes. If there's more than
 */
public class TiffROIPixelsContentAccess implements ContentAccess {

    private static final Logger LOG = LoggerFactory.getLogger(TiffROIPixelsContentAccess.class);

    @Override
    public boolean isAccessTypeSupported(String contentAccessType) {
        return "TIFF_ROI_PIXELS".equalsIgnoreCase(contentAccessType);
    }

    @Override
    public long estimateContentSize(List<ContentNode> contentNodes, ContentAccessParams contentAccessParams, ContentStreamReader contentObjectReader) {
        Integer xCenter = contentAccessParams.getAsInt("xCenter", 0);
        Integer yCenter = contentAccessParams.getAsInt("yCenter", 0);
        Integer zCenter = contentAccessParams.getAsInt("zCenter", 0);
        Integer dimX = contentAccessParams.getAsInt("dimX", -1);
        Integer dimY = contentAccessParams.getAsInt("dimY", -1);
        Integer dimZ = contentAccessParams.getAsInt("dimZ", -1);
        try {
            if (CollectionUtils.isEmpty(contentNodes)) {
                return 0L;
            } else if (contentNodes.size() == 1) {
                try (InputStream nodeContentStream = contentObjectReader.readContent(contentNodes.get(0).getObjectKey())) {
                    return ImageUtils.sizeImagePixelBytesFromTiffStream(
                            nodeContentStream,
                            xCenter, yCenter, zCenter,
                            dimX, dimY, dimZ
                    );
                }
            } else {
                long totalSize = 2 * TarConstants.DEFAULT_RCDSIZE;
                for (ContentNode contentNode : contentNodes) {
                    try (InputStream nodeContentStream = contentObjectReader.readContent(contentNode.getObjectKey())) {
                        long entrySize = ImageUtils.sizeImagePixelBytesFromTiffStream(
                                nodeContentStream,
                                xCenter, yCenter, zCenter,
                                dimX, dimY, dimZ
                        );
                        totalSize += ContentNodeHelper.calculateTarEntrySize(entrySize);
                    }
                }
                return totalSize;
            }
        } catch (Exception e) {
            throw new ContentException(e);
        }
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes,
                                ContentAccessParams contentAccessParams,
                                ContentStreamReader contentObjectReader,
                                OutputStream outputStream) {
        Integer xCenter = contentAccessParams.getAsInt("xCenter", 0);
        Integer yCenter = contentAccessParams.getAsInt("yCenter", 0);
        Integer zCenter = contentAccessParams.getAsInt("zCenter", 0);
        Integer dimX = contentAccessParams.getAsInt("dimX", -1);
        Integer dimY = contentAccessParams.getAsInt("dimY", -1);
        Integer dimZ = contentAccessParams.getAsInt("dimZ", -1);

        try {
            if (CollectionUtils.isEmpty(contentNodes)) {
                return 0L;
            } else if (contentNodes.size() == 1) {
                try (InputStream nodeContentStream = contentObjectReader.readContent(contentNodes.get(0).getObjectKey())) {
                    return IOStreamUtils.copyFrom(ImageUtils.loadImagePixelBytesFromTiffStream(
                            nodeContentStream,
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
                    try (InputStream nodeContent = contentObjectReader.readContent(contentNode.getObjectKey())) {
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
