package org.janelia.jacsstorage.io.contenthandlers.tiff;

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
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public class TiffROIPixelsContentConverter implements ContentConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TiffROIPixelsContentConverter.class);
    private static final String FILTER_TYPE = "TIFF_ROI_PIXELS";
    private static final byte[] EMPTY_BYTES = new byte[]{};

    @Override
    public boolean support(String filterType) {
        return FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    @TimedMethod(
            argList = {0},
            logResult = true
    )
    @Override
    public long convertContent(DataContent dataContent, OutputStream outputStream) {
        Integer xCenter = dataContent.getContentFilterParams().getAsInt("xCenter", 0);
        Integer yCenter = dataContent.getContentFilterParams().getAsInt("yCenter", 0);
        Integer zCenter = dataContent.getContentFilterParams().getAsInt("zCenter", 0);
        Integer dimX = dataContent.getContentFilterParams().getAsInt("dimX", -1);
        Integer dimY = dataContent.getContentFilterParams().getAsInt("dimY", -1);
        Integer dimZ = dataContent.getContentFilterParams().getAsInt("dimZ", -1);

        List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(2).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(peekDataNodes)) {
            return 0L;
        } else if (peekDataNodes.size() == 1) {
            return IOStreamUtils.copyFrom(ImageUtils.loadImagePixelBytesFromTiffStream(
                    dataContent.streamDataNode(peekDataNodes.get(0)),
                    xCenter, yCenter, zCenter,
                    dimX, dimY, dimZ
            ), outputStream);
        } else {
            TarArchiveOutputStream archiveOutputStream = dataContent.streamDataNodes()
                    .sorted(DataContentUtils.getDataNodePathComparator())
                    .reduce(
                            new TarArchiveOutputStream(outputStream, TarConstants.DEFAULT_RCDSIZE),
                            (archiveStream, dn) -> {
                                Path entryPath = Paths.get(dn.getNodeRelativePath());
                                String entryName;
                                byte[] entryBytes;
                                if (dn.isCollectionFlag()) {
                                    entryName = StringUtils.appendIfMissing(
                                            StringUtils.prependIfMissing(
                                                    StringUtils.prependIfMissing(entryPath.toString(), "/"), "."),
                                            "/"); // append '/' for directory entries
                                    entryBytes = EMPTY_BYTES;
                                } else {
                                    entryName = StringUtils.prependIfMissing(
                                            StringUtils.prependIfMissing(entryPath.toString(), "/"),
                                            ".");
                                    entryBytes = ImageUtils.loadImagePixelBytesFromTiffStream(
                                            dataContent.streamDataNode(dn),
                                            xCenter, yCenter, zCenter,
                                            dimX, dimY, dimZ
                                    );
                                }
                                TarArchiveEntry entry = new TarArchiveEntry(entryName);
                                entry.setSize(entryBytes == null ? 0 : entryBytes.length);
                                try {
                                    archiveStream.putArchiveEntry(entry);
                                    if (!dn.isCollectionFlag()) {
                                        IOStreamUtils.copyFrom(entryBytes, archiveStream);
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
    }

    @Override
    public long estimateContentSize(DataContent dataContent) {
        Integer xCenter = dataContent.getContentFilterParams().getAsInt("xCenter", 0);
        Integer yCenter = dataContent.getContentFilterParams().getAsInt("yCenter", 0);
        Integer zCenter = dataContent.getContentFilterParams().getAsInt("zCenter", 0);
        Integer dimX = dataContent.getContentFilterParams().getAsInt("dimX", -1);
        Integer dimY = dataContent.getContentFilterParams().getAsInt("dimY", -1);
        Integer dimZ = dataContent.getContentFilterParams().getAsInt("dimZ", -1);
        List<DataNodeInfo> peekDataNodes = dataContent.streamDataNodes().filter(dn -> !dn.isCollectionFlag()).limit(2).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(peekDataNodes)) {
            return 0L;
        } else if (peekDataNodes.size() == 1) {
            return ImageUtils.sizeImagePixelBytesFromTiffStream(
                    dataContent.streamDataNode(peekDataNodes.get(0)),
                    xCenter, yCenter, zCenter,
                    dimX, dimY, dimZ
            );
        } else {
            return dataContent.streamDataNodes()
                    .sorted(DataContentUtils.getDataNodePathComparator())
                    .reduce(
                            0L,
                            (size, dn) -> {
                                long entrySize;
                                if (dn.isCollectionFlag()) {
                                    entrySize = 0L;
                                } else {
                                    entrySize = ImageUtils.sizeImagePixelBytesFromTiffStream(
                                            dataContent.streamDataNode(dn),
                                            xCenter, yCenter, zCenter,
                                            dimX, dimY, dimZ
                                    );
                                }
                                return size + DataContentUtils.calculateTarEntrySize(entrySize);
                            },
                            (s1, s2) -> s1 + s2);
        }
    }
}
