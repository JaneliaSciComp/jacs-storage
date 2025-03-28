package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.janelia.jacsstorage.service.ContentException;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentMetadataReader;
import org.janelia.rendering.RenderedImageInfo;
import org.janelia.rendering.utils.ImageUtils;

public class TiffMetadataReader implements ContentMetadataReader {

    @Override
    public boolean support(String mimeType) {
        return StringUtils.equalsIgnoreCase(mimeType, "image/tiff");
    }

    @Override
    public Map<String, Object> getMetadata(ContentNode contentNode, ContentStreamReader contentObjectReader) {
        try (InputStream is = contentObjectReader.getContentInputStream(contentNode.getObjectKey())) {
            RenderedImageInfo imageInfo = ImageUtils.loadImageInfoFromTiffStream(is);
            long size = (long) (imageInfo.sx * imageInfo.sy * imageInfo.sz * imageInfo.getBytesPerPixel());
            return ImmutableMap.<String, Object>builder()
                    .put("size", size)
                    .put("sx", imageInfo.sx)
                    .put("sy", imageInfo.sy)
                    .put("sz", imageInfo.sz)
                    .put("cmPixelSize", imageInfo.cmPixelSize)
                    .put("sRGBspace", imageInfo.sRGBspace)
                    .build();
        } catch (IOException e) {
            throw new ContentException(e);
        }
    }
}
