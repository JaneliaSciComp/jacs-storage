package org.janelia.jacsstorage.service.impl;

import java.io.OutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.janelia.jacsstorage.service.ContentAccessParams;
import org.janelia.jacsstorage.service.ContentGetter;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;

public class ContentGetterImpl implements ContentGetter {

    private final ContentStorageService contentStorageService;
    private final ContentAccessProvider contentAccessProvider;
    private final List<ContentNode> contentNodes;
    private final ContentAccessParams contentAccessParams;

    ContentGetterImpl(ContentStorageService contentStorageService,
                      ContentAccessProvider contentAccessProvider,
                      List<ContentNode> contentNodes,
                      ContentAccessParams contentAccessParams) {
        this.contentStorageService = contentStorageService;
        this.contentAccessProvider = contentAccessProvider;
        this.contentNodes = contentNodes;
        this.contentAccessParams = contentAccessParams;
    }

    @Override
    public long estimateContentSize() {
        ContentAccess contentAccess = contentAccessProvider.getContentFilter(contentAccessParams);
        return contentAccess.estimateContentSize(contentNodes, contentAccessParams, contentStorageService);
    }

    @Override
    public Map<String, Object> getMetaData() {
        if (contentNodes.isEmpty()) {
            return Collections.emptyMap();
        } else if (contentNodes.size() == 1) {
            // if there's exactly 1 match lookup if there's a supported metada reader
            ContentMetadataReader contentMetadataReader = contentAccessProvider.getContentMetadataReader(contentNodes.get(0).getMimeType());
            return contentMetadataReader.getMetadata(contentNodes.get(0), contentStorageService);
        }
        return Collections.emptyMap(); // !!!! FIXME
    }

    @Override
    public List<ContentNode> getObjectsList() {
        return contentNodes;
    }

    @Override
    public long streamContent(OutputStream outputStream) {
        ContentAccess contentAccess = contentAccessProvider.getContentFilter(contentAccessParams);
        return contentAccess.retrieveContent(contentNodes, contentAccessParams, contentStorageService, outputStream);
    }
}
