package org.janelia.jacsstorage.service.impl.contenthandling.tiff;

import java.io.OutputStream;
import java.util.List;

import org.janelia.jacsstorage.coreutils.IOStreamUtils;
import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStreamReader;
import org.janelia.jacsstorage.service.impl.ContentAccess;
import org.janelia.rendering.NamedSupplier;
import org.janelia.rendering.utils.ImageUtils;

public class TiffMergeBandsContentAccess implements ContentAccess {

    @Override
    public boolean isAccessTypeSupported(String contentAccessType) {
        return "TIFF_MERGE_BANDS".equalsIgnoreCase(contentAccessType);
    }

    @Override
    public long estimateContentSize(List<ContentNode> contentNodes,
                                    ContentAccessParams contentAccessParams,
                                    ContentStreamReader contentObjectReader) {
        if (contentNodes.isEmpty()) {
            return 0L;
        }
        Integer pageNumber = contentAccessParams.getAsInt("z", 0);
        return ImageUtils.sizeBandMergedTextureBytesFromImageStreams(
                contentNodes.stream()
                        .filter(ContentNode::isNotCollection)
                        .map(n -> NamedSupplier.namedSupplier(
                                n.getName(),
                                () -> contentObjectReader.getContentInputStream(n.getObjectKey()))),
                pageNumber
        );
    }

    @Override
    public long retrieveContent(List<ContentNode> contentNodes,
                                ContentAccessParams contentAccessParams,
                                ContentStreamReader contentObjectReader,
                                OutputStream outputStream) {
        if (contentNodes.isEmpty()) {
            return 0L;
        }
        Integer pageNumber = contentAccessParams.getAsInt("z", 0);
        byte[] contentBytes = ImageUtils.bandMergedTextureBytesFromImageStreams(
                contentNodes.stream()
                        .filter(ContentNode::isNotCollection)
                        .map(n -> NamedSupplier.namedSupplier(
                                n.getName(),
                                () -> contentObjectReader.getContentInputStream(n.getObjectKey()))),
                pageNumber
        );
        if (contentBytes == null) {
            return 0L;
        } else {
            return IOStreamUtils.copyFrom(contentBytes, outputStream);
        }
    }

}
