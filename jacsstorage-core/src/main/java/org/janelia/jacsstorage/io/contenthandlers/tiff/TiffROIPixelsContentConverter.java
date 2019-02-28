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
        List<DataNodeInfo> dataNodes = dataContent.listDataNodes();
        Integer xCenter = dataContent.getContentFilterParams().getAsInt("xCenter", 0);
        Integer yCenter = dataContent.getContentFilterParams().getAsInt("yCenter", 0);
        Integer zCenter = dataContent.getContentFilterParams().getAsInt("zCenter", 0);
        Integer dimX = dataContent.getContentFilterParams().getAsInt("dimX", -1);
        Integer dimY = dataContent.getContentFilterParams().getAsInt("dimY", -1);
        Integer dimZ = dataContent.getContentFilterParams().getAsInt("dimZ", -1);
        if (CollectionUtils.isEmpty(dataNodes)) {
            return 0L;
        } else if (dataNodes.size() == 1) {
            if (!dataNodes.get(0).isCollectionFlag()) {
                return IOStreamUtils.copyFrom(ImageUtils.loadImagePixelBytesFromTiffStream(
                        dataContent.streamDataNode(dataNodes.get(0)),
                        xCenter, yCenter, zCenter,
                        dimX, dimY, dimZ
                ), outputStream);
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
                                            dataContent.streamDataNode(dataNodes.get(0)),
                                            xCenter, yCenter, zCenter,
                                            dimX, dimY, dimZ
                                    );
                                }
                                TarArchiveEntry entry = new TarArchiveEntry(entryName);
                                entry.setSize(entryBytes == null ? 0 : entryBytes.length);
                                long nbytes;
                                try {
                                    p.getLeft().putArchiveEntry(entry);
                                    if (!dn.isCollectionFlag()) {
                                        nbytes = IOStreamUtils.copyFrom(entryBytes, p.getLeft());
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
