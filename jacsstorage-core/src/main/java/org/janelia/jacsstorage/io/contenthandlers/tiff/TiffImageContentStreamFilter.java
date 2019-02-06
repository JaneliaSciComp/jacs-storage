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
