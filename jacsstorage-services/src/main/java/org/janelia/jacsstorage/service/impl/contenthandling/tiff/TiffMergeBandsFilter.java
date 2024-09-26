package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentFilter;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;

public class TiffMergeBandsFilter implements ContentFilter {

    private static final String FILTER_TYPE = "TIFF_MERGE_BANDS";

    @Override
    public boolean support(String filterType) {
        return FILTER_TYPE.equalsIgnoreCase(filterType);
    }

    @Override
    public long applyContentFilter(ContentFilterParams filterParams, List<ContentNode> contentNodes, OutputStream outputStream) {
        Integer pageNumber = filterParams.getAsInt("z", 0);
        byte[] contentBytes = ImageUtils.bandMergedTextureBytesFromImageStreams(
                contentNodes.stream()
                        .map(n -> NamedSupplier.namedSupplier(
                                n.getName(),
                                n::getContent)),
                pageNumber
        );
        if (contentBytes == null) {
            return 0L;
        } else {
            return IOStreamUtils.copyFrom(contentBytes, outputStream);
        }
    }

}
