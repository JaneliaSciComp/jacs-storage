package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.util.Map;

import javax.enterprise.inject.Vetoed;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentMetadataReader;
import org.janelia.rendering.RenderedImageInfo;
import org.janelia.rendering.utils.ImageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TiffMetadataReader implements ContentMetadataReader {

    private static final Logger LOG = LoggerFactory.getLogger(TiffMetadataReader.class);

    @Override
    public boolean support(String mimeType) {
        return StringUtils.equalsIgnoreCase(mimeType, "image/tiff");
    }

    @Override
    public Map<String, Object> getMetadata(ContentNode contentNode) {
        RenderedImageInfo imageInfo = ImageUtils.loadImageInfoFromTiffStream(contentNode.getContent());
        long size = (long) (imageInfo.sx * imageInfo.sy * imageInfo.sz * imageInfo.getBytesPerPixel());
        return ImmutableMap.<String, Object>builder()
                .put("size", size)
                .put("sx", imageInfo.sx)
                .put("sy", imageInfo.sy)
                .put("sz", imageInfo.sz)
                .put("cmPixelSize", imageInfo.cmPixelSize)
                .put("sRGBspace", imageInfo.sRGBspace)
                .build();
    }
}
