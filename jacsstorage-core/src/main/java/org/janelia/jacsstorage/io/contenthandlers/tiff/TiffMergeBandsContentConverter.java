package org.janelia.jacsstorage.io.contenthandlers.tiff;

import java.io.OutputStream;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.io.DataContentUtils;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;

public class TiffMergeBandsContentConverter implements ContentConverter {

    private static final String FILTER_TYPE = "TIFF_MERGE_BANDS";

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
        Integer pageNumber = dataContent.getContentFilterParams().getAsInt("z", 0);
        byte[] contentBytes = ImageUtils.bandMergedTextureBytesFromImageStreams(
                dataContent.streamDataNodes()
                        .filter(dn -> !dn.isCollectionFlag())
                        .sorted(DataContentUtils.getDataNodePathComparator()
                                .thenComparing(DataNodeInfo::getNodeRelativePath))
                        .map(dn -> NamedSupplier.namedSupplier(
                                dn.getNodeAccessURL(),
                                () -> dataContent.streamDataNode(dn))),
                pageNumber
        );
        if (contentBytes == null) {
            return 0L;
        } else {
            return IOStreamUtils.copyFrom(contentBytes, outputStream);
        }
    }

    @Override
    public long estimateContentSize(DataContent dataContent) {
        Integer pageNumber = dataContent.getContentFilterParams().getAsInt("z", 0);
        return ImageUtils.sizeBandMergedTextureBytesFromImageStreams(
                dataContent.streamDataNodes()
                        .filter(dn -> !dn.isCollectionFlag())
                        .sorted(DataContentUtils.getDataNodePathComparator()
                                .thenComparing(DataNodeInfo::getNodeRelativePath))
                        .map(dn -> NamedSupplier.namedSupplier(
                                dn.getNodeAccessURL(),
                                () -> dataContent.streamDataNode(dn))),
                pageNumber
        );
    }
}
