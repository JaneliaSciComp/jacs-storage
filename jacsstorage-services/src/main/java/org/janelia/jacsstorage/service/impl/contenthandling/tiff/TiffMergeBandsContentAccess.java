package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.io.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;

public class TiffMergeBandsContentAccess implements ContentAccess {

    @Override
    public boolean isSupportedAccessType(String contentAccessType) {
        return "TIFF_MERGE_BANDS".equalsIgnoreCase(contentAccessType);
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes, ContentAccessParams filterParams, OutputStream outputStream) {
        if (contentNodes.isEmpty()) {
            return 0L;
        }
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
