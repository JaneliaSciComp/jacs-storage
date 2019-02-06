package org.janelia.jacsstorage.io.contenthandlers.tiff;

import com.google.common.collect.ImmutableMap;
import org.janelia.jacsstorage.io.ContentFilteredInputStream;
import org.janelia.jacsstorage.io.ContentInfoExtractor;
import org.janelia.rendering.utils.ImageInfo;
import org.janelia.rendering.utils.ImageUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

public class TiffImageContentInfoExtractor implements ContentInfoExtractor {

    @Override
    public boolean support(String mimeType) {
        return "image/tiff".equalsIgnoreCase(mimeType);
    }

    @Override
    public Map<String, Object> extractContentInfo(InputStream inputStream) {
        ImageInfo imageInfo = ImageUtils.loadImageInfoFromTiffStream(inputStream);
        return ImmutableMap.<String, Object>builder()
                .put("sx", imageInfo.sx)
                .put("sy", imageInfo.sy)
                .put("sz", imageInfo.sz)
                .put("bitsPerPixel", imageInfo.bitsPerPixel)
                .put("cmPixelSize", imageInfo.cmPixelSize)
                .put("sRGBspace", imageInfo.sRGBspace)
                .build();
    }

    public ContentFilteredInputStream apply(ContentFilteredInputStream stream) {
        Integer z0 = stream.getContentFilterParams().getAsInt("z0", 0);
        Integer deltaZ = stream.getContentFilterParams().getAsInt("deltaz", -1);

        byte[] imageBytes = ImageUtils.loadRenderedImageBytesFromTiffStream(stream.getUnderlyingStream(), z0, deltaZ);
        if (imageBytes == null) {
            return new ContentFilteredInputStream(stream.getContentFilterParams(), new ByteArrayInputStream(new byte[0]));
        } else {
            return new ContentFilteredInputStream(stream.getContentFilterParams(), new ByteArrayInputStream(imageBytes));
        }
    }
}
