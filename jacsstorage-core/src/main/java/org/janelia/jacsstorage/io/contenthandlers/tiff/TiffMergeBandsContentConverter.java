package org.janelia.jacsstorage.io.contenthandlers.tiff;

import java.io.OutputStream;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.io.DataContentUtils;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiffMergeBandsContentConverter implements ContentConverter {

    private static final Logger LOG = LoggerFactory.getLogger(TiffMergeBandsContentConverter.class);
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
        List<DataNodeInfo> dataNodes = dataContent.listDataNodes();
        if (CollectionUtils.isEmpty(dataNodes)) {
            return 0L;
        } else {
            Integer pageNumber = dataContent.getContentFilterParams().getAsInt("z", 0);
            byte[] contentBytes = ImageUtils.bandMergedTextureBytesFromImageStreams(
                    dataNodes.stream()
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
    }

    @Override
    public long estimateContentSize(DataContent dataContent) {
        List<DataNodeInfo> dataNodes = dataContent.listDataNodes();
        if (CollectionUtils.isEmpty(dataNodes)) {
            return 0L;
        } else {
            // for size always estimate it for page 0 since it should be the same size.
            return ImageUtils.sizeBandMergedTextureBytesFromImageStreams(
                    dataNodes.stream()
                            .filter(dn -> !dn.isCollectionFlag())
                            .sorted(DataContentUtils.getDataNodePathComparator()
                                    .thenComparing(DataNodeInfo::getNodeRelativePath))
                            .map(dn -> NamedSupplier.namedSupplier(
                                    dn.getNodeAccessURL(),
                                    () -> dataContent.streamDataNode(dn))),
                    0
            );
        }
    }
}
