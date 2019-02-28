package org.janelia.jacsstorage.io.contenthandlers.tiff;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.interceptors.annotations.TimedMethod;
import org.janelia.jacsstorage.io.ContentInfoExtractor;
import org.janelia.rendering.RenderedImageInfo;
import org.janelia.rendering.utils.ImageUtils;

import java.io.InputStream;
import java.util.Map;

public class TiffImageContentInfoExtractor implements ContentInfoExtractor {

    @Override
    public boolean support(String mimeType) {
        return "image/tiff".equalsIgnoreCase(mimeType);
    }

    @TimedMethod(
            logArgs = false
    )
    @Override
    public Map<String, Object> extractContentInfo(InputStream inputStream) {
        RenderedImageInfo imageInfo = ImageUtils.loadImageInfoFromTiffStream(inputStream);
        return ImmutableMap.<String, Object>builder()
                .put("sx", imageInfo.sx)
                .put("sy", imageInfo.sy)
                .put("sz", imageInfo.sz)
                .put("cmPixelSize", imageInfo.cmPixelSize)
                .put("sRGBspace", imageInfo.sRGBspace)
                .build();
    }

}
