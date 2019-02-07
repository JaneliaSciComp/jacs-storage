package org.janelia.jacsstorage.io.contenthandlers.tiff;

import org.janelia.jacsstorage.io.ContentFilteredInputStream;
import org.janelia.jacsstorage.io.ContentStreamFilter;
import org.janelia.rendering.utils.ImageUtils;

import java.io.ByteArrayInputStream;

public class TiffImageContentStreamFilter implements ContentStreamFilter {

    private final String TIFF_FILTER_TYPE = "TIFF_IMAGE";

    @Override
    public boolean support(String filterType) {
        return TIFF_FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    @Override
    public ContentFilteredInputStream apply(ContentFilteredInputStream stream) {
        Integer x0 = stream.getContentFilterParams().getAsInt("x0", 0);
        Integer y0 = stream.getContentFilterParams().getAsInt("y0", 0);
        Integer z0 = stream.getContentFilterParams().getAsInt("z0", 0);
        Integer deltaX = stream.getContentFilterParams().getAsInt("deltax", -1);
        Integer deltaY = stream.getContentFilterParams().getAsInt("deltay", -1);
        Integer deltaZ = stream.getContentFilterParams().getAsInt("deltaz", -1);

        byte[] imageBytes = ImageUtils.loadRenderedImageBytesFromTiffStream(
                stream.getUnderlyingStream(),
                x0, y0, z0,
                deltaX, deltaY, deltaZ);
        if (imageBytes == null) {
            return new ContentFilteredInputStream(stream.getContentFilterParams(), new ByteArrayInputStream(new byte[0]));
        } else {
            return new ContentFilteredInputStream(stream.getContentFilterParams(), new ByteArrayInputStream(imageBytes));
        }
    }
}
