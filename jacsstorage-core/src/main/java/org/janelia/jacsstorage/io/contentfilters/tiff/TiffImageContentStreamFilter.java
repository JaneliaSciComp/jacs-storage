package org.janelia.jacsstorage.io.contentfilters.tiff;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.io.ContentInputStream;
import org.janelia.jacsstorage.io.ContentStreamFilter;
import org.janelia.model.util.ImageUtils;

import javax.enterprise.inject.Vetoed;
import java.io.ByteArrayInputStream;

public class TiffImageContentStreamFilter implements ContentStreamFilter {

    private final String TIFF_FILTER_TYPE = "TIFF_IMAGE";

    @Override
    public boolean support(String filterType) {
        return TIFF_FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    public ContentInputStream apply(ContentFilterParams filterParams, ContentInputStream stream) {
        Integer x0 = filterParams.getAsInt("x0", 0);
        Integer y0 = filterParams.getAsInt("y0", 0);
        Integer z0 = filterParams.getAsInt("z0", 0);
        Integer deltaX = filterParams.getAsInt("deltax", -1);
        Integer deltaY = filterParams.getAsInt("deltay", -1);
        Integer deltaZ = filterParams.getAsInt("deltaz", -1);

        byte[] imageBytes = ImageUtils.loadImageFromTiffStream(stream, x0, y0, z0, deltaX, deltaY, deltaZ);
        if (imageBytes == null) {
            return new ContentInputStream(stream.getContentEntryName(), new ByteArrayInputStream(new byte[0]));
        } else {
            return new ContentInputStream(stream.getContentEntryName(), new ByteArrayInputStream(imageBytes));
        }
    }
}
