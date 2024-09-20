package org.janelia.jacsstorage.service.impl;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.inject.Inject;

import org.janelia.jacsstorage.io.ContentFilterParams;
import org.janelia.jacsstorage.model.jacsstorage.JADEStorageURI;
import org.janelia.jacsstorage.service.ContentNode;
import org.janelia.jacsstorage.service.ContentStorageService;

public class DataStorageServiceImpl {

    private final ContentStorageServiceProvider contentStorageServiceProvider;
    private final ContentFilterProvider contentFilterProvider;

    @Inject
    DataStorageServiceImpl(ContentStorageServiceProvider contentStorageServiceProvider,
                           ContentFilterProvider contentFilterProvider) {
        this.contentStorageServiceProvider = contentStorageServiceProvider;
        this.contentFilterProvider = contentFilterProvider;
    }

    public long writeDataStream(JADEStorageURI storageURI, InputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        return contentStorageService.writeContent(storageURI.getStorageKey(), dataStream);
    }

    public long readDataStream(JADEStorageURI storageURI, ContentFilterParams filterParams, OutputStream dataStream) {
        ContentStorageService contentStorageService = contentStorageServiceProvider.getStorageService(storageURI);
        List<ContentNode> contentNodes = contentStorageService.listContentNodes(storageURI.getStorageKey(), filterParams);
        ContentFilter contentFilter = contentFilterProvider.getContentFilter(filterParams);
        return contentFilter.applyContentFilter(filterParams, contentNodes, dataStream);
    }

}
