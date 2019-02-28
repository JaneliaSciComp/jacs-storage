package org.janelia.jacsstorage.io.contenthandlers.tiff;

import org.apache.commons.collections4.CollectionUtils;
import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.datarequest.DataNodeInfo;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentConverter;
import org.janelia.jacsstorage.io.DataContent;
import org.janelia.jacsstorage.io.DataContentUtils;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.image.RenderedImage;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;

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
            Integer z = dataContent.getContentFilterParams().getAsInt("z", 0);
            return ImageUtils.mergeImageBands(dataNodes.stream()
                    .sorted(DataContentUtils.getDataNodePathComparator())
                    .filter(dn -> !dn.isCollectionFlag())
                    .map(dn -> () -> {
                        RenderedImage rim = ImageUtils.loadRenderedImageFromTiffStream(dataContent.streamDataNode(dn), z);
                        if (rim == null) {
                            return Optional.empty();
                        } else {
                            return Optional.of(rim);
                        }
                    }))
                    .map(imageBytes -> IOStreamUtils.copyFrom(imageBytes, outputStream))
                    .orElse(0L);
        }
    }

}
