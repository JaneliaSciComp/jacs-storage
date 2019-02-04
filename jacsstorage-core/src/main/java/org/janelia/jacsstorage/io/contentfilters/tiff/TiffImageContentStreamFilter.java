package org.janelia.jacsstorage.io.contentfilters.tiff;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.ContentInputStream;
import org.janelia.jacsstorage.io.ContentStreamFilter;
import org.janelia.rendering.utils.ImageUtils;

import java.io.ByteArrayInputStream;

public class TiffImageContentStreamFilter implements ContentStreamFilter {

    private final String TIFF_FILTER_TYPE = "TIFF_IMAGE";

    @Override
    public boolean support(String filterType) {
        return TIFF_FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    public ContentInputStream apply(ContentFilterParams filterParams, ContentInputStream stream) {
        Integer z0 = filterParams.getAsInt("z0", 0);
        Integer deltaZ = filterParams.getAsInt("deltaz", -1);

        byte[] imageBytes = ImageUtils.loadRenderedImageBytesFromTiffStream(stream, z0, deltaZ);
        if (imageBytes == null) {
            return new ContentInputStream(stream.getContentEntryName(), new ByteArrayInputStream(new byte[0]));
        } else {
            return new ContentInputStream(stream.getContentEntryName(), new ByteArrayInputStream(imageBytes));
        }
    }
}
